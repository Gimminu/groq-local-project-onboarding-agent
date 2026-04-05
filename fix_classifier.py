import re
with open("/Users/giminu0930/projects/workspace/groq-mcp-mac-agent/code/app/index_v2/classifier.py", "r") as f:
    content = f.read()

# 1. Update _llm_prompt_payload
# Find the start of "allowed": {
start_idx = content.find('"allowed": {')
# Find the start of "constraints": {
end_idx = content.find('"constraints": {')

if start_idx != -1 and end_idx != -1:
    new_allowed = '"allowed": {\n                "modes": list(PLACEMENT_MODES),\n            },\n            '
    content = content[:start_idx] + new_allowed + content[end_idx:]

# Update output schema
schema_old = '"output_schema": [\n                "anchor",\n                "mode",\n                "target_path",\n                "space",\n                "stream",\n                "domain",\n                "focus",\n                "asset_type",\n                "confidence",\n                "rationale",\n                "alternatives",\n                "depth_score",\n            ]'
schema_new = '"output_schema": [\n                "placement_mode",\n                "target_path",\n                "create_folders",\n                "confidence",\n                "reason",\n                "alternatives",\n            ]'
content = content.replace(schema_old, schema_new)

# 2. Update _sanitize_llm_payload
def sanitize_replacement(match):
    return """def _sanitize_llm_payload(self, payload: dict[str, Any], *, current: ClassificationResult) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return current.to_dict()

        llm_mode = str(payload.get("placement_mode") or payload.get("mode") or "review_only").strip().lower()
        if llm_mode not in PLACEMENT_MODES + ("keep_here",):
            llm_mode = "review_only"

        review_only_requested = (llm_mode == "review_only")
        target_path = payload.get("target_path")
        create_folders = payload.get("create_folders", [])
        if not isinstance(create_folders, list):
            create_folders = []

        try:
            confidence = float(payload.get("confidence", current.confidence))
        except (TypeError, ValueError):
            confidence = current.confidence
        confidence = max(0.0, min(1.0, confidence))

        target_segments = self._normalized_target_segments(target_path)
        depth_level = "ok"
        depth_candidate = len(target_segments)
        if depth_candidate >= 5:
            depth_level = "fail"
            llm_mode = "review_only"
            confidence = min(confidence, 0.25)
        elif depth_candidate == 4:
            depth_level = "warning"
            confidence = max(0.0, confidence - 0.1)

        blocked_segments = set(GENERIC_BLOCKED_SEGMENTS)
        has_blocked_segment = any(segment in blocked_segments for segment in target_segments)
        generic_path_blocked = False
        if self._is_generic_core_path(target_segments):
            generic_path_blocked = True
            llm_mode = "review_only"
            confidence = min(confidence, 0.3)
        elif has_blocked_segment:
            confidence = min(confidence, 0.65)

        if llm_mode == "review_only":
            confidence = min(confidence, 0.5)

        rationale = str(payload.get("reason", payload.get("rationale", "llm fallback")))
        alternatives = payload.get("alternatives", ())
        if not isinstance(alternatives, (list, tuple)):
            alternatives = ()

        return {
            "placement_mode": llm_mode,
            "target_path": target_path,
            "create_folders": tuple(create_folders),
            "confidence": confidence,
            "rationale": rationale,
            "source": "llm",
            "alternatives": tuple(alternatives),
            "review_required": review_only_requested or confidence < 0.75,
            "metadata": {
                "llm_mode": llm_mode,
                "generic_path_blocked": generic_path_blocked,
                "depth_score": depth_candidate,
            }
        }
"""

pattern = re.compile(r'def _sanitize_llm_payload\(self, payload: dict\[str, Any\], \*, current: ClassificationResult\) -> dict\[str, Any\]:.*?(?=    def _review_result)', re.DOTALL)
content = pattern.sub(sanitize_replacement, content)

with open("/Users/giminu0930/projects/workspace/groq-mcp-mac-agent/code/app/index_v2/classifier.py", "w") as f:
    f.write(content)
