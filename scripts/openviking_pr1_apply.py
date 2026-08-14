from pathlib import Path

path = Path("plugins/memory/openviking/__init__.py")
text = path.read_text(encoding="utf-8")

new_memory_write = '''    def on_memory_write(
        self,
        action: str,
        target: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Mirror successful built-in memory mutations to OpenViking."""
        if action not in {"add", "replace", "remove"} or not self._ensure_client():
            return
        if action in {"add", "replace"} and not content:
            return

        subdir = _MEMORY_WRITE_TARGET_SUBDIR_MAP.get(target, _DEFAULT_MEMORY_SUBDIR)
        from plugins.memory.openviking.native_memory_mirror import (
            enqueue_native_memory_write,
        )

        enqueue_native_memory_write(
            self,
            action,
            target,
            content,
            metadata=metadata,
            subdir=subdir,
        )

'''

if "enqueue_native_memory_write(" not in text:
    start_marker = "    def on_memory_write(\n"
    end_marker = "    def get_tool_schemas("
    start = text.find(start_marker)
    end = text.find(end_marker, start + len(start_marker))
    if start < 0 or end < 0:
        raise SystemExit("could not locate on_memory_write method boundaries")
    text = text[:start] + new_memory_write + text[end:]

if "shutdown_native_memory_mirror(self, timeout=5.0)" not in text:
    shutdown_marker = "    def shutdown(self) -> None:\n"
    shutdown_start = text.find(shutdown_marker)
    if shutdown_start < 0:
        raise SystemExit("could not locate shutdown method")
    assignment = "        self._shutting_down = True\n"
    assignment_pos = text.find(assignment, shutdown_start)
    if assignment_pos < 0:
        raise SystemExit("could not locate shutdown state assignment")
    insert_at = assignment_pos + len(assignment)
    addition = '''        from plugins.memory.openviking.native_memory_mirror import (
            shutdown_native_memory_mirror,
        )

        shutdown_native_memory_mirror(self, timeout=5.0)
'''
    text = text[:insert_at] + addition + text[insert_at:]

path.write_text(text, encoding="utf-8")
