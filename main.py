from __future__ import annotations

import asyncio
import json
import os
import shlex
import signal
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator

from fastmcp import FastMCP
from poke import Poke
from poke.mcp import PokeCallbackMiddleware, with_callbacks

mcp = FastMCP("Poke Codex Bridge")


@dataclass
class ManagedProcess:
    id: str
    command: list[str]
    started_at: float = field(default_factory=time.time)
    process: asyncio.subprocess.Process | None = None
    output: deque[str] = field(default_factory=lambda: deque(maxlen=500))
    queue: asyncio.Queue[str] = field(default_factory=asyncio.Queue)
    completed: bool = False
    returncode: int | None = None


codex_sessions: dict[str, ManagedProcess] = {}
poke_tunnel: ManagedProcess | None = None


def _append(proc: ManagedProcess, msg: str) -> None:
    proc.output.append(msg)
    proc.queue.put_nowait(msg)


async def _pump_stream(
    proc: ManagedProcess,
    stream: asyncio.StreamReader | None,
    prefix: str,
) -> None:
    if stream is None:
        return

    while True:
        line = await stream.readline()
        if not line:
            return
        text = f"{prefix}{line.decode(errors='replace').rstrip()}"
        _append(proc, text)


async def _monitor(proc: ManagedProcess) -> None:
    assert proc.process is not None

    stdout_task = asyncio.create_task(_pump_stream(proc, proc.process.stdout, "stdout: "))
    stderr_task = asyncio.create_task(_pump_stream(proc, proc.process.stderr, "stderr: "))

    rc = await proc.process.wait()
    await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)

    proc.completed = True
    proc.returncode = rc
    _append(proc, f"session_exit: returncode={rc}")


async def _start_process(command: list[str]) -> ManagedProcess:
    proc_id = str(uuid.uuid4())
    managed = ManagedProcess(id=proc_id, command=command)

    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    managed.process = process
    asyncio.create_task(_monitor(managed))

    _append(managed, f"started: {' '.join(command)}")
    return managed


async def _start_poke_tunnel_on_boot(host: str, port: int, path: str) -> None:
    global poke_tunnel
    local_mcp_url = os.environ.get("POKE_MCP_LOCAL_URL", f"http://{host}:{port}{path}")
    tunnel_name = os.environ.get("POKE_TUNNEL_NAME", "Local Codex MCP")

    command = ["npx", "poke@latest", "tunnel", local_mcp_url, "-n", tunnel_name]
    poke_tunnel = await _start_process(command)

    startup_message = os.environ.get(
        "POKE_CONNECTED_MESSAGE",
        f"Codex is connected to local MCP bridge at {local_mcp_url}.",
    )

    try:
        Poke().send_message(startup_message)
        _append(poke_tunnel, "poke_notify: sent Codex connected message")
    except Exception as exc:
        _append(poke_tunnel, f"poke_notify_error: {exc}")


async def _stop_poke_tunnel_on_shutdown() -> None:
    if poke_tunnel is None or poke_tunnel.process is None or poke_tunnel.completed:
        return

    poke_tunnel.process.send_signal(signal.SIGINT)
    await asyncio.wait_for(poke_tunnel.process.wait(), timeout=10)


@mcp.tool()
@with_callbacks
async def run_codex_stream(
    prompt: str,
    cwd: str | None = None,
    model: str | None = None,
    extra_args: list[str] | None = None,
) -> AsyncGenerator[str, None]:
    """Run `codex exec` and stream line-by-line output; compatible with Poke callback streaming."""
    codex_cmd = shlex.split(os.environ.get("CODEX_CMD", "codex"))
    command = [*codex_cmd, "exec", "--json", prompt]

    if cwd:
        command.extend(["--cwd", cwd])
    if model:
        command.extend(["--model", model])
    if extra_args:
        command.extend(extra_args)

    managed = await _start_process(command)
    codex_sessions[managed.id] = managed

    yield json.dumps({"session_id": managed.id, "event": "started"})

    while True:
        line = await managed.queue.get()
        yield json.dumps({"session_id": managed.id, "event": "output", "line": line})
        if managed.completed and managed.queue.empty():
            break


@mcp.tool
async def interrupt_codex_session(session_id: str) -> dict[str, Any]:
    """Interrupt a running `codex exec` process by session id."""
    proc = codex_sessions.get(session_id)
    if not proc or not proc.process:
        return {"ok": False, "error": "Unknown session_id"}
    if proc.completed:
        return {"ok": True, "status": "already_completed", "returncode": proc.returncode}

    proc.process.send_signal(signal.SIGINT)
    await asyncio.wait_for(proc.process.wait(), timeout=10)
    return {"ok": True, "status": "interrupted"}


@mcp.tool
async def list_codex_sessions() -> list[dict[str, Any]]:
    """List managed Codex sessions and recent output."""
    return [
        {
            "session_id": p.id,
            "command": p.command,
            "completed": p.completed,
            "returncode": p.returncode,
            "recent_output": list(p.output)[-50:],
        }
        for p in codex_sessions.values()
    ]


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "3000"))
    path = os.environ.get("MCP_PATH", "/mcp")

    app = mcp.http_app(path=path, transport="streamable-http")

    async def _on_startup() -> None:
        await _start_poke_tunnel_on_boot(host, port, path)

    app.add_event_handler("startup", _on_startup)
    app.add_event_handler("shutdown", _stop_poke_tunnel_on_shutdown)

    wrapped_app = PokeCallbackMiddleware(app)

    import uvicorn

    uvicorn.run(wrapped_app, host=host, port=port)


if __name__ == "__main__":
    main()
