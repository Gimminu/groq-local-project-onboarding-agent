from __future__ import annotations

import json
import os
import re
import unicodedata
import warnings
from pathlib import Path
from typing import Any

from app.errors import AppError
from app.index_v2.content_hints import (
    extract_content_hint,
    infer_bundle_focus_name,
    semantic_tokens_for_path,
    semantic_tokens_from_sources,
)
from app.index_v2.db import IndexDatabase
from app.index_v2.focus_rules import infer_focus_from_path
from app.index_v2.llm_controller import (
    LLMDeferredDecision,
    LLMProviderController,
    LLMProviderError,
    LLMRateLimitError,
)
from app.index_v2.naming import normalize_filename, normalize_segment
from app.index_v2.semantic_policy import SemanticDomainPolicy
from app.index_v2.types import ClassificationResult, IndexOrganizerConfig, IndexedNode

try:
    from groq import Groq
except ImportError:  # pragma: no cover - optional at runtime
    Groq = None

try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        import google.generativeai as genai
except ImportError:  # pragma: no cover - optional at runtime
    genai = None

try:
    import urllib.request as _urllib_request

    _OLLAMA_AVAILABLE = True
except ImportError:  # pragma: no cover
    _OLLAMA_AVAILABLE = False

CODE_EXTENSIONS = {
    ".py",
    ".js",
    ".ts",
    ".tsx",
    ".jsx",
    ".java",
    ".kt",
    ".cpp",
    ".cc",
    ".c",
    ".h",
    ".hpp",
    ".cs",
    ".rb",
    ".go",
    ".rs",
    ".php",
    ".swift",
    ".scala",
    ".sh",
    ".zsh",
    ".bash",
    ".sql",
    ".ino",
    ".ipynb",
    ".code-workspace",
}
DOC_EXTENSIONS = {".pdf", ".doc", ".docx", ".hwp", ".hwpx", ".pages"}
SLIDE_EXTENSIONS = {".ppt", ".pptx", ".key"}
NOTE_EXTENSIONS = {".md", ".txt", ".rtf"}
DATA_EXTENSIONS = {".csv", ".tsv", ".xls", ".xlsx", ".json", ".parquet"}
ASSET_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".mp4", ".mov", ".avi", ".wav", ".mp3", ".m4a"}
INSTALLER_EXTENSIONS = {".dmg", ".pkg", ".exe", ".msi", ".crx"}
ARCHIVE_EXTENSIONS = {".zip", ".rar", ".7z", ".tar", ".gz", ".bz2"}
FORM_KEYWORDS = {"form", "application", "contract", "agreement", "consent", "template", "신청서", "서식", "동의서", "양식"}
TRANSIENT_SYSTEM_PREFIXES = ("~$",)
INCOMPLETE_SUFFIXES = (".download", ".crdownload", ".part", ".tmp", ".partial")
ARCHIVE_BUCKET_MARKERS = ("압축원본", "archive")
NUMBERED_TOP_LEVEL_PATTERN = re.compile(r"^(?P<number>\d{3})(?:[_\-\s].+)?$")
NUMBERED_SUBTOPIC_PATTERN = re.compile(r"^(?P<number>\d{2})(?:[_\-\s].+)?$")
PERIOD_TOKEN_PATTERN = re.compile(r"(?P<year>20\d{2})(?:[-_\s년]?(?P<month>0[1-9]|1[0-2]))?")
TEMP_BUCKET_MARKERS = ("임시", "temp", "tmp")
SYSTEM_DEPENDENCY_NAMES = {
    ".git",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    "site-packages",
    "dist",
    "build",
}
ADAPTIVE_CONTEXT_IGNORED_SEGMENTS = {
    "download",
    "downloads",
    "desktop",
    "documents",
    "watch",
    "watcher",
    "staging",
    "stage",
    "organizer",
    "live",
    "test",
    "tmp",
    "temp",
    "inbox",
}
ADAPTIVE_CONTEXT_NOISE_PATTERN = re.compile(r"(?:^|[_\-])(?:tmp|temp|test|live|watch|stage|staging|organizer)(?:[_\-]|$)")
PROJECT_STRONG_DIR_HINTS = {".git", ".venv", "node_modules", ".pio"}
PROJECT_STRUCTURE_HINTS = {
    "src",
    "app",
    "apps",
    "lib",
    "pkg",
    "cmd",
    "tests",
    "test",
    "frontend",
    "backend",
    "client",
    "server",
    "firmware",
    "include",
    ".github",
}
PROJECT_CONFIG_HINTS = {
    "vite.config.js",
    "vite.config.ts",
    "vite.config.mjs",
    "next.config.js",
    "next.config.mjs",
    "next.config.ts",
    "tsconfig.json",
    "tsconfig.base.json",
    "turbo.json",
    "nx.json",
    "docker-compose.yml",
    "docker-compose.yaml",
    "compose.yml",
    "compose.yaml",
    "setup.py",
    "setup.cfg",
    "pytest.ini",
    "tox.ini",
    ".python-version",
    ".nvmrc",
}
GENERIC_SEGMENTS = {
    "general",
    "misc",
    "temp",
    "other",
    "stuff",
    "category",
    "categories",
    "etc",
    "unnamed",
    "new-folder",
    "new_folder",
    "folder",
}
REVIEW_MOVABLE_ASSET_TYPES = {"docs", "slides", "notes", "forms", "data", "assets"}
METADATA_ARTIFACT_PATTERN = re.compile(r"(?i)(?:^\.ds_store$|^thumbs\.db$|ds store)")
PROJECT_DOMAIN_HINTS = {
    "workspace": ("workspace", "hub", "suite"),
    "embedded": ("embedded", "arduino", "platformio", "robot", "firmware", "iot", "pio"),
    "experiments": ("experiment", "experiments", "lab", "sandbox", "prototype", "pytorch", "c-project"),
}
LLM_CLASSIFIER_SYSTEM_PROMPT = """
You are a file placement engine. Your goal is NOT to build a taxonomy.
Your only goal is: minimize the user's future re-find cost (clicks + uncertainty).

You MUST NOT require any fixed top-level anchors (e.g., Projects/Research/etc).
Top-level folders may be created dynamically ONLY when necessary and beneficial.

INPUT YOU GET:
- base_dir
- item_path, filename, extension, size, timestamps
- optional short content hints
- existing_tree_summary
- protected_paths

STEP 0 — Hard protection:
If item is inside or is a dependency/system folder, DO NOT MOVE.
Examples: node_modules, .venv, site-packages, __pycache__, dist, build.
Return placement_mode = review_only or keep_here. confidence = 1.0.

STEP 1 — Choose PLACEMENT MODE (only one):
- direct
- single_file_folder
- merge_existing
- review_only

STEP 2 — Generate candidates.
At most one candidate may create a NEW top-level folder.
Never create generic names like general/misc/temp/other/etc.

STEP 3 — Score candidates and pick best.
Minimize depth + generic_names + redundancy + ambiguity.
Depth target is 2-3. Depth 4 is warning. Depth >= 5 is FAIL.
Avoid meaningless middle containers like "code" or "docs" unless they add real meaning.

STEP 4 — Output STRICT JSON ONLY.
{
  "placement_mode": "direct|single_file_folder|merge_existing|review_only|keep_here",
  "target_path": "relative/path/from/base_dir or null",
  "create_folders": ["relative/path/to/create"],
  "confidence": 0.0,
  "reason": "1-2 short sentences",
  "alternatives": [
    {"placement_mode": "...", "target_path": "...", "why_not": "..."}
  ]
}
""".strip()


class IndexClassifier:
    def __init__(self, config: IndexOrganizerConfig, database: IndexDatabase, semantic_policy: SemanticDomainPolicy) -> None:
        self.config = config
        self.database = database
        self.semantic_policy = semantic_policy
        self._groq_client = None
        self._gemini_client = None
        self._batch_command: str | None = None
        self._watch_cloud_invocations = 0
        self._content_hint_cache: dict[Path, str] = {}
        self._directory_hit_cache: dict[str, int] = {}
        provider_env = os.getenv("LLM_PROVIDER")
        if provider_env:
            self._preferred_provider = provider_env.strip().lower()
        else:
            self._preferred_provider = self.config.llm.preferred_provider
        self._llm_controller = LLMProviderController(
            config=self.config,
            database=self.database,
            groq_call=self._call_groq,
            gemini_call=self._call_gemini,
            ollama_call=self._call_ollama,
            ollama_healthcheck=self._ollama_server_available,
        )

    def begin_batch(self, command: str) -> None:
        self._batch_command = command
        self._watch_cloud_invocations = 0
        self._directory_hit_cache = {}

    def classify(self, node: IndexedNode) -> ClassificationResult:
        if node.is_symlink:
            return self._review_result(0.0, "symlink is indexed but never followed", asset_type=self._asset_type_from_node(node))
        if self._is_transient_system_file(node.path):
            return ClassificationResult(
                placement_mode="keep_here",
                target_path=None,
                confidence=1.0,
                rationale="transient Office/Finder lock file is ignored",
                source="rule",
                review_required=False,
                metadata={"transient_system_file": True, "gate_state": "incomplete_or_transient"},
                stream="system",
                domain="system",
                focus="system",
                asset_type=self.config.naming.misc_asset_type,
            )
        if self._is_system_dependency(node.path):
            return ClassificationResult(
                placement_mode="keep_here",
                target_path=None,
                confidence=1.0,
                rationale="dependency or system path is protected from organizer moves",
                source="rule",
                review_required=False,
                metadata={"system_dependency": True, "gate_state": "system_or_dependency"},
                stream="system",
                domain="system",
                focus="system",
                asset_type=self._asset_type_from_node(node),
            )

        canonical = self._classify_from_canonical_path(node)
        if canonical is not None:
            return canonical

        seed = self._rule_based(node)
        if self.config.llm.enable_llm_first:
            if self._llm_runtime_ready() and self._should_try_llm_fallback(node, seed):
                return self._llm_fallback(node, seed)
            return seed

        if seed.confidence >= self.config.groq.confidence_threshold:
            return seed
        if not self._llm_runtime_ready():
            return seed
        if not self._should_try_llm_fallback(node, seed):
            return seed
        return self._llm_fallback(node, seed)

    def normalized_name(self, node: IndexedNode, classification: ClassificationResult) -> str | None:
        if node.kind != "file":
            return None
        return normalize_filename(node.path, classification, self.config).filename

    def _classify_from_canonical_path(self, node: IndexedNode) -> ClassificationResult | None:
        if not _is_relative_to(node.path, self.config.spaces_root):
            return None
        try:
            relative = node.path.relative_to(self.config.spaces_root)
        except ValueError:
            return None

        adaptive_existing = self._adaptive_existing_path_result(node)
        if adaptive_existing is not None:
            return adaptive_existing

        if self.config.is_protected_project_internal(node.path):
            return ClassificationResult(
                placement_mode="keep_here",
                target_path=None,
                confidence=1.0,
                rationale="protected project internal path already lives in the managed tree",
                source="path",
                review_required=False,
                metadata={"protected_project_internal": True, "gate_state": "system_or_dependency"},
            )

        candidate = relative if node.kind == "dir" else relative.parent
        if self._looks_like_invalid_canonical(candidate):
            return ClassificationResult(
                placement_mode="review_only",
                target_path=self._review_target_for_asset(self._asset_type_from_node(node)),
                confidence=0.2,
                rationale="invalid managed-tree wrapper requires review normalization",
                source="path",
                review_required=True,
                metadata={"invalid_canonical": True},
                domain=self.config.naming.unknown_domain,
            )

        parsed = self.config.parse_canonical_relative(candidate)
        if parsed is None:
            return None

        space, stream, domain, focus, asset_type = parsed
        return ClassificationResult(
            placement_mode="direct",
            target_path=str(candidate).replace("\\", "/"),
            confidence=0.99,
            rationale="path already matches the managed canonical tree",
            source="path",
            review_required=False,
            metadata={"derived_space": space, "derived_stream": stream, "derived_domain": domain, "derived_focus": focus, "derived_asset_type": asset_type},
            space=space,
            stream=stream,
            domain=domain,
            focus=focus,
            asset_type=asset_type,
        )

    def _looks_like_invalid_canonical(self, relative: Path) -> bool:
        parts = relative.parts
        if not parts:
            return False
        if len(parts) >= 2 and parts[0] == "review" and parts[1] == "imports":
            return True
        if parts[0] in self.config.spaces:
            return True
        return False

    def _content_hint(self, path: Path) -> str:
        cached = self._content_hint_cache.get(path)
        if cached is not None:
            return cached
        hint = extract_content_hint(path)
        self._content_hint_cache[path] = hint
        return hint

    def _semantic_tokens(self, path: Path) -> set[str]:
        return semantic_tokens_for_path(path, extra_stopwords=self.config.generic_tokens)

    def _suggest_domain_focus(self, *, node: IndexedNode, stream: str, domain: str, asset_type: str) -> str | None:
        focus = infer_focus_from_path(
            node.path,
            stream=stream,
            domain=domain,
            asset_type=asset_type,
            config=self.config,
            hint_text=self._content_hint(node.path),
        )
        if focus:
            return focus
        existing_target = self._existing_domain_target(node=node, domain=domain, asset_type=asset_type, stream=stream)
        if existing_target:
            parts = [part for part in existing_target.split("/") if part]
            if len(parts) >= 4:
                return parts[2]
        return None

    def _suggest_review_focus(self, *, node: IndexedNode, asset_type: str, preferred: str | None = None) -> str | None:
        focus = infer_focus_from_path(
            node.path,
            stream="review",
            domain="review",
            asset_type=asset_type,
            config=self.config,
            hint_text=self._content_hint(node.path),
        )
        if focus:
            return focus
        existing_focus = self._best_existing_review_focus(node=node, asset_type=asset_type)
        if existing_focus:
            return existing_focus
        if preferred:
            preferred_focus = self._suggest_domain_focus(
                node=node,
                stream=self._default_stream_for_domain(preferred),
                domain=preferred,
                asset_type=asset_type,
            )
            if preferred_focus and not self.semantic_policy.is_banned_generic_name(preferred_focus):
                return preferred_focus
            if asset_type not in REVIEW_MOVABLE_ASSET_TYPES and asset_type != self.config.naming.misc_asset_type:
                return None
            normalized = normalize_segment(
                preferred,
                self.config.naming.delimiter,
                self.config.naming.max_segment_length,
            )
            if normalized and not self.semantic_policy.is_banned_generic_name(normalized):
                return normalized
        return None

    def _review_destination_root_for(self, node: IndexedNode) -> str | None:
        if _is_relative_to(node.path, self.config.adaptive_review_root):
            return "review_staging"
        return None

    def _adaptive_existing_path_result(self, node: IndexedNode) -> ClassificationResult | None:
        if not self.config.adaptive_mode_enabled() or not self.config.adaptive_placement.stabilize_existing_spaces_items:
            return None
        if not _is_relative_to(node.path, self.config.spaces_root):
            return None
        try:
            relative = node.path.relative_to(self.config.spaces_root)
        except ValueError:
            return None
        if not relative.parts:
            return None
        if node.kind == "file" and len(relative.parts) == 1:
            return None
        candidate = relative if node.kind == "dir" else relative.parent
        if not candidate.parts:
            return None
        return ClassificationResult(
            placement_mode="keep_here",
            target_path=str(candidate).replace("\\", "/"),
            confidence=1.0,
            rationale="existing adaptive placement is treated as stable and not refiled automatically",
            source="path",
            review_required=False,
            metadata={"adaptive_stable_path": True},
            stream="adaptive",
            domain=candidate.parts[0],
            focus=candidate.parts[-1],
            asset_type=self._asset_type_from_node(node),
        )

    def _adaptive_rule_based(self, node: IndexedNode, asset_type: str) -> ClassificationResult | None:
        if not self.config.adaptive_mode_enabled():
            return None
        if _is_relative_to(node.path, self.config.spaces_root):
            return None
        in_hidden_review = _is_relative_to(node.path, self.config.adaptive_review_root)

        archive_target = self._adaptive_archive_target(node=node, asset_type=asset_type)
        if archive_target is not None:
            return ClassificationResult(
                placement_mode="merge_existing",
                target_path=archive_target,
                confidence=0.93,
                rationale="archive file was routed into an existing archive-original bucket to preserve established structure",
                source="heuristic",
                review_required=False,
                metadata={"adaptive_archive_bucket": True},
                stream="adaptive",
                domain=archive_target.split("/", 1)[0],
                focus=archive_target.rsplit("/", 1)[-1],
                asset_type=asset_type,
            )

        numbered_target = self._adaptive_numbered_taxonomy_target(node=node, asset_type=asset_type)
        if numbered_target is not None:
            target_path, placement_mode, metadata = numbered_target
            confidence = 0.91 if placement_mode == "merge_existing" else 0.86
            rationale = (
                "matched existing numbered topic hierarchy using filename/content signals"
                if placement_mode == "merge_existing"
                else "created a numbered subtopic under a matched top-level topic"
            )
            return ClassificationResult(
                placement_mode=placement_mode,
                target_path=target_path,
                confidence=confidence,
                rationale=rationale,
                source="heuristic",
                review_required=False,
                metadata=metadata,
                stream="adaptive",
                domain=target_path.split("/", 1)[0],
                focus=target_path.rsplit("/", 1)[-1],
                asset_type=asset_type,
            )

        preferred_focus = self._adaptive_preferred_focus(node=node, asset_type=asset_type)

        existing_target = self._adaptive_existing_target(
            node=node,
            asset_type=asset_type,
            preferred_focus=preferred_focus,
        )
        if existing_target is not None:
            return ClassificationResult(
                placement_mode="merge_existing",
                target_path=existing_target,
                confidence=0.9,
                rationale="merged into an existing similar folder using filename and content similarity",
                source="heuristic",
                review_required=False,
                metadata={"adaptive_match": True},
                stream="adaptive",
                domain=existing_target.split("/", 1)[0],
                focus=existing_target.rsplit("/", 1)[-1],
                asset_type=asset_type,
            )

        if node.kind == "dir":
            folder_name = self._adaptive_new_top_level_name(node)
            if folder_name:
                return ClassificationResult(
                    placement_mode="direct",
                    target_path=folder_name,
                    confidence=0.84,
                    rationale="directory-sized incoming bundle creates a new topic folder in adaptive mode",
                    source="heuristic",
                    review_required=False,
                    metadata={"adaptive_created_folder": True},
                    stream="adaptive",
                    domain=folder_name,
                    focus=folder_name,
                    asset_type=asset_type,
                )

        if in_hidden_review:
            return None

        hidden_review_confidence = 0.52 if asset_type in REVIEW_MOVABLE_ASSET_TYPES or asset_type == self.config.naming.misc_asset_type else 0.3
        return self._review_result(
            confidence=hidden_review_confidence,
            rationale="ambiguous adaptive placement is staged in hidden review until a better destination is proven",
            asset_type=asset_type,
            focus=preferred_focus,
            destination_root="review_staging",
            metadata={"adaptive_review": True},
        )

    def _adaptive_archive_target(self, *, node: IndexedNode, asset_type: str) -> str | None:
        if asset_type != "archives" or node.kind != "file":
            return None
        if _is_relative_to(node.path, self.config.adaptive_review_root):
            return None
        if not self._is_under_watch_root(node.path):
            return None

        archive_roots = self._archive_bucket_roots()
        if not archive_roots:
            return None

        node_text = _normalized_archive_match_text(node.path.stem)
        best: tuple[int, int, Path] | None = None
        for root in archive_roots:
            candidate_dirs = [root]
            try:
                children = sorted((child for child in root.iterdir() if child.is_dir()), key=lambda path: path.name.lower())
            except OSError:
                children = []
            candidate_dirs.extend(children)

            for candidate in candidate_dirs:
                score = _archive_candidate_score(candidate_name=candidate.name, node_text=node_text)
                if candidate != root and score <= 0:
                    continue
                prefer_child = 1 if candidate != root else 0
                key = (score, prefer_child, candidate)
                if best is None or key > best:
                    best = key

        if best is None:
            if len(archive_roots) != 1:
                return None
            chosen = archive_roots[0]
        else:
            score, _, chosen = best
            if score <= 0 and len(archive_roots) != 1:
                return None

        try:
            return str(chosen.relative_to(self.config.spaces_root)).replace("\\", "/")
        except ValueError:
            return None

    def _archive_bucket_roots(self) -> list[Path]:
        root = self.config.spaces_root
        if not root.exists() or not root.is_dir():
            return []
        buckets: list[Path] = []
        try:
            children = sorted((child for child in root.iterdir() if child.is_dir()), key=lambda path: path.name.lower())
        except OSError:
            return []
        for child in children:
            if child.name in self.config.ignore_names:
                continue
            if _looks_like_archive_bucket_name(child.name):
                buckets.append(child)
        return buckets

    def _adaptive_numbered_taxonomy_target(
        self,
        *,
        node: IndexedNode,
        asset_type: str,
    ) -> tuple[str, str, dict[str, Any]] | None:
        if node.kind != "file":
            return None
        if _is_relative_to(node.path, self.config.spaces_root):
            return None
        if not self._is_under_watch_root(node.path):
            return None

        top_roots = self._topic_top_level_roots()
        if not top_roots:
            return None

        prefer_numbered = self._prefer_numbered_taxonomy(top_roots)

        top_root = self._best_numbered_top_level(
            node=node,
            asset_type=asset_type,
            roots=top_roots,
            prefer_numbered=prefer_numbered,
        )
        if top_root is None:
            return None

        best_subtopic = self._best_numbered_subtopic(
            node=node,
            asset_type=asset_type,
            top_root=top_root,
            prefer_numbered=prefer_numbered,
        )
        if best_subtopic is not None:
            target = str(best_subtopic.relative_to(self.config.spaces_root)).replace("\\", "/")
            return (
                target,
                "merge_existing",
                {
                    "adaptive_numbered_taxonomy": True,
                    "numbered_top_level": top_root.name,
                    "numbered_subtopic": best_subtopic.name,
                    "taxonomy_mode": "existing-folder-analysis",
                    "numbered_mode": "merge_existing",
                },
            )

        label = self._derive_numbered_subtopic_label(node=node, asset_type=asset_type)
        if not label:
            return None
        if prefer_numbered or any(NUMBERED_SUBTOPIC_PATTERN.match(child.name) for child in _iter_dir_children(top_root)):
            next_number = self._next_numbered_subtopic_number(top_root)
            created = top_root / f"{next_number:02d}_{label}"
        else:
            created = top_root / label
        target = str(created.relative_to(self.config.spaces_root)).replace("\\", "/")
        return (
            target,
            "direct",
            {
                "adaptive_numbered_taxonomy": True,
                "numbered_top_level": top_root.name,
                "numbered_subtopic": created.name,
                "taxonomy_mode": "existing-folder-analysis",
                "numbered_mode": "direct",
            },
        )

    def _topic_top_level_roots(self) -> list[Path]:
        root = self.config.spaces_root
        if not root.exists() or not root.is_dir():
            return []
        try:
            children = sorted((child for child in root.iterdir() if child.is_dir()), key=lambda path: path.name.lower())
        except OSError:
            return []
        managed_roots = {name.lower() for name in self.config.managed_root_names()}
        roots: list[Path] = []
        for child in children:
            lowered = child.name.lower()
            if child.name in self.config.ignore_names:
                continue
            if lowered in managed_roots:
                continue
            if child.name.startswith("."):
                continue
            roots.append(child)
        return roots

    def _prefer_numbered_taxonomy(self, roots: list[Path]) -> bool:
        numbered_count = sum(1 for root in roots if NUMBERED_TOP_LEVEL_PATTERN.match(root.name))
        return numbered_count >= 2 and (numbered_count * 2) >= max(1, len(roots))

    def _numbered_top_level_roots(self) -> list[Path]:
        root = self.config.spaces_root
        if not root.exists() or not root.is_dir():
            return []
        try:
            children = sorted((child for child in root.iterdir() if child.is_dir()), key=lambda path: path.name.lower())
        except OSError:
            return []
        return [child for child in children if NUMBERED_TOP_LEVEL_PATTERN.match(child.name)]

    def _best_numbered_top_level(
        self,
        *,
        node: IndexedNode,
        asset_type: str,
        roots: list[Path],
        prefer_numbered: bool,
    ) -> Path | None:
        node_tokens = self._adaptive_node_tokens(node)
        source_text = self._adaptive_source_text(node)
        allow_temp_bucket = _looks_like_temp_source(node.path)
        best: tuple[int, int, int, str, Path] | None = None
        for root in roots:
            if _looks_like_temp_bucket(root.name) and not allow_temp_bucket:
                continue
            if self._should_skip_topic_root_for_asset(root=root, asset_type=asset_type, source_path=node.path):
                continue
            root_tokens = _adaptive_tokens_for_name(root.name)
            score = _fuzzy_token_overlap_score(node_tokens, root_tokens)
            score += _topic_hint_score(source_text=source_text, target_name=root.name, asset_type=asset_type)
            if prefer_numbered and NUMBERED_TOP_LEVEL_PATTERN.match(root.name):
                score += 2
            elif prefer_numbered:
                score -= 1
            if score <= 0:
                continue
            hit_count = self._directory_hit_count(root)
            prefix = _numbered_prefix_value(root.name, width=3)
            key = (score, hit_count, -prefix, str(root), root)
            if best is None or key > best:
                best = key
        if best is None:
            return None
        return best[4]

    def _best_numbered_subtopic(
        self,
        *,
        node: IndexedNode,
        asset_type: str,
        top_root: Path,
        prefer_numbered: bool,
    ) -> Path | None:
        try:
            subtopics = sorted(
                (
                    child
                    for child in top_root.iterdir()
                    if child.is_dir() and (
                        NUMBERED_SUBTOPIC_PATTERN.match(child.name)
                        if prefer_numbered
                        else child.name not in self.config.ignore_names and not child.name.startswith(".")
                    )
                ),
                key=lambda path: path.name.lower(),
            )
        except OSError:
            return None
        if not subtopics:
            return None

        node_tokens = self._adaptive_node_tokens(node)
        source_text = self._adaptive_source_text(node)
        best: tuple[int, int, int, str, Path] | None = None
        for subtopic in subtopics:
            target_tokens = _adaptive_tokens_for_name(subtopic.name)
            score = _fuzzy_token_overlap_score(node_tokens, target_tokens)
            score += _topic_hint_score(source_text=source_text, target_name=subtopic.name, asset_type=asset_type)
            if prefer_numbered and NUMBERED_SUBTOPIC_PATTERN.match(subtopic.name):
                score += 1
            if score <= 0:
                continue
            hit_count = self._directory_hit_count(subtopic)
            prefix = _numbered_prefix_value(subtopic.name, width=2)
            key = (score, hit_count, -prefix, str(subtopic), subtopic)
            if best is None or key > best:
                best = key
        if best is None:
            return None
        return best[4]

    def _adaptive_node_tokens(self, node: IndexedNode) -> set[str]:
        tokens = self._semantic_tokens(node.path)
        if not tokens:
            tokens = _path_tokens(node.path.stem)
        tokens.update(_adaptive_tokens_for_name(node.path.stem))
        tokens.update(self._adaptive_watch_context_tokens(node.path))
        return {token for token in tokens if token}

    def _adaptive_source_text(self, node: IndexedNode) -> str:
        context_text = " ".join(sorted(self._adaptive_watch_context_tokens(node.path)))
        return _normalized_archive_match_text(f"{node.path.stem}\n{self._content_hint(node.path)}\n{context_text}")

    def _adaptive_watch_context_tokens(self, path: Path) -> set[str]:
        context_tokens: set[str] = set()
        root = self._watch_root_for_path(path)
        if root is None:
            return context_tokens
        try:
            relative = path.resolve(strict=False).relative_to(root.resolve(strict=False))
        except ValueError:
            try:
                relative = path.relative_to(root)
            except ValueError:
                return context_tokens
        parts = list(relative.parts[:-1])
        if not parts:
            return context_tokens
        for segment in parts[-4:]:
            if self._adaptive_context_segment_ignored(segment):
                continue
            context_tokens.update(_adaptive_tokens_for_name(segment))
        return {token for token in context_tokens if token and not _is_short_numeric_token(token)}

    def _watch_root_for_path(self, path: Path) -> Path | None:
        path_resolved = path.resolve(strict=False)
        for root in sorted(self.config.watch_roots, key=lambda item: len(str(item)), reverse=True):
            if _is_relative_to(path_resolved, root):
                return root
        return None

    def _adaptive_context_segment_ignored(self, value: str) -> bool:
        normalized = unicodedata.normalize("NFKC", str(value)).strip().lower()
        if not normalized:
            return True
        if normalized in ADAPTIVE_CONTEXT_IGNORED_SEGMENTS:
            return True
        if normalized.startswith("."):
            return True
        if ADAPTIVE_CONTEXT_NOISE_PATTERN.search(normalized):
            return True
        tokenized = normalized.replace("-", "_")
        parts = [part for part in tokenized.split("_") if part]
        if parts and all(part.isdigit() for part in parts):
            return True
        return False

    def _derive_numbered_subtopic_label(self, *, node: IndexedNode, asset_type: str) -> str | None:
        focus = self._suggest_review_focus(node=node, asset_type=asset_type)
        source_text = _normalized_archive_match_text(f"{node.path.stem}\n{self._content_hint(node.path)}")
        doc_group = _document_group_label(source_text=source_text, asset_type=asset_type)
        period = _extract_period_token(source_text)

        base = focus or doc_group
        if not base:
            base = normalize_segment(node.path.stem, self.config.naming.delimiter, self.config.naming.max_segment_length)
        if not base:
            return None
        if period:
            base = f"{base}_{period}"
        return normalize_segment(base, self.config.naming.delimiter, self.config.naming.max_segment_length)

    def _next_numbered_subtopic_number(self, top_root: Path) -> int:
        numbers: list[int] = []
        try:
            children = [child for child in top_root.iterdir() if child.is_dir()]
        except OSError:
            children = []
        for child in children:
            value = _numbered_prefix_value(child.name, width=2)
            if value > 0:
                numbers.append(value)
        if not numbers:
            return 1
        return min(99, max(numbers) + 1)

    def _numbered_top_level_name(self, label: str) -> str:
        roots = self._numbered_top_level_roots()
        if not roots or not self._prefer_numbered_taxonomy(self._topic_top_level_roots()):
            return label
        if NUMBERED_TOP_LEVEL_PATTERN.match(label):
            return label
        numbers = [value for value in (_numbered_prefix_value(root.name, width=3) for root in roots) if value > 0]
        next_number = 1 if not numbers else min(999, max(numbers) + 1)
        return f"{next_number:03d}_{label}"

    def _should_skip_topic_root_for_asset(self, *, root: Path, asset_type: str, source_path: Path) -> bool:
        normalized = unicodedata.normalize("NFKC", root.name).lower()
        source_normalized = unicodedata.normalize("NFKC", str(source_path)).lower()
        if "obsidian" in normalized and "obsidian" not in source_normalized:
            return True
        if asset_type != "code" and _looks_like_project_collection_root(root, self.config.project_markers):
            return True
        if asset_type not in {"notes", "docs", "forms"} and any(token in normalized for token in ("template", "tags", "hubs", "calendar")):
            return True
        return False

    def _directory_hit_count(self, directory: Path) -> int:
        marker = str(directory.resolve(strict=False))
        cached = self._directory_hit_cache.get(marker)
        if cached is not None:
            return cached
        count = 0
        try:
            for child in directory.rglob("*"):
                if child.name in self.config.ignore_names:
                    continue
                if child.is_file():
                    count += 1
                if count >= 400:
                    break
        except OSError:
            count = 0
        self._directory_hit_cache[marker] = count
        return count

    def _adaptive_existing_target(
        self,
        *,
        node: IndexedNode,
        asset_type: str,
        preferred_focus: str | None = None,
    ) -> str | None:
        best: tuple[int, int, str, Path] | None = None
        node_tokens = self._semantic_tokens(node.path)
        if not node_tokens:
            node_tokens = _path_tokens(node.path.stem)
        for candidate_dir in self._iter_adaptive_candidate_dirs(asset_type=asset_type):
            if preferred_focus and not self._adaptive_candidate_matches_focus(candidate_dir, preferred_focus):
                continue
            candidate_tokens = self._adaptive_candidate_tokens(candidate_dir)
            if not candidate_tokens:
                continue
            score = _fuzzy_token_overlap_score(node_tokens, candidate_tokens)
            if score < self.config.adaptive_placement.min_existing_folder_score:
                continue
            depth = len(candidate_dir.relative_to(self.config.spaces_root).parts)
            hit_count = self._directory_hit_count(candidate_dir)
            key = (score, hit_count, -depth, str(candidate_dir), candidate_dir)
            if best is None or key > best:
                best = key
        if best is None:
            return None
        return str(best[4].relative_to(self.config.spaces_root)).replace("\\", "/")

    def _adaptive_preferred_focus(self, *, node: IndexedNode, asset_type: str) -> str | None:
        focus = self._suggest_review_focus(node=node, asset_type=asset_type)
        if not focus or self.semantic_policy.is_banned_generic_name(focus):
            return None
        if asset_type in REVIEW_MOVABLE_ASSET_TYPES or asset_type in {"archives", "code"}:
            return focus
        return None

    def _adaptive_candidate_matches_focus(self, directory: Path, preferred_focus: str) -> bool:
        try:
            relative = directory.relative_to(self.config.spaces_root)
        except ValueError:
            return False
        normalized_focus = normalize_segment(
            preferred_focus,
            self.config.naming.delimiter,
            self.config.naming.max_segment_length,
        )
        if not normalized_focus:
            return False
        for part in relative.parts:
            if part in self.config.managed_root_names() or part in self.config.asset_types:
                continue
            normalized_part = normalize_segment(
                part,
                self.config.naming.delimiter,
                self.config.naming.max_segment_length,
            )
            if normalized_part == normalized_focus:
                return True
        return False

    def _iter_adaptive_candidate_dirs(self, *, asset_type: str) -> list[Path]:
        root = self.config.spaces_root
        max_depth = max(1, int(self.config.adaptive_placement.max_existing_folder_depth))
        candidates: list[Path] = []
        seen: set[str] = set()
        if not root.exists():
            return candidates
        for current_root, dirnames, _ in os.walk(root, topdown=True):
            current = Path(current_root)
            try:
                relative = current.relative_to(root)
            except ValueError:
                continue
            depth = len(relative.parts)
            dirnames[:] = [
                name
                for name in sorted(dirnames)
                if name not in self.config.ignore_names
                and not name.startswith(".")
                and depth < max_depth
            ]
            if depth == 0:
                continue
            if len(relative.parts) == 1 and relative.parts[0] in self.config.managed_root_names():
                continue
            target_dir = current
            if current.name not in self.config.asset_types:
                asset_dir = current / asset_type
                if asset_dir.exists() and asset_dir.is_dir():
                    target_dir = asset_dir
            marker = str(target_dir)
            if marker in seen:
                continue
            seen.add(marker)
            candidates.append(target_dir)
        return candidates

    def _adaptive_candidate_tokens(self, directory: Path) -> set[str]:
        try:
            relative = directory.relative_to(self.config.spaces_root)
        except ValueError:
            return set()
        values: list[str] = []
        for index, part in enumerate(relative.parts):
            if index == 0 and part in self.config.managed_root_names():
                continue
            if part in self.config.asset_types:
                continue
            if self.semantic_policy.is_banned_generic_name(part):
                continue
            values.append(part)
        try:
            child_names = [
                child.name
                for child in sorted(directory.iterdir(), key=lambda path: path.name.lower())[:12]
                if child.name not in self.config.ignore_names and not child.name.startswith(".")
            ]
        except OSError:
            child_names = []
        values.extend(child_names)
        return semantic_tokens_from_sources(values, extra_stopwords=self.config.generic_tokens)

    def _adaptive_new_top_level_name(self, node: IndexedNode) -> str | None:
        source_name = normalize_segment(
            node.path.name,
            self.config.naming.delimiter,
            self.config.naming.max_segment_length,
        )
        if (
            source_name
            and source_name not in self.config.managed_root_names()
            and source_name not in self.config.asset_types
            and not self._adaptive_name_is_blocked_for_top_level(source_name)
            and not self.semantic_policy.is_banned_generic_name(source_name)
        ):
            return self._numbered_top_level_name(source_name)

        files: list[Path] = []
        if node.path.is_dir():
            try:
                for child in node.path.rglob("*"):
                    if child.is_file() and child.name not in self.config.ignore_names:
                        files.append(child)
                    if len(files) >= 12:
                        break
            except OSError:
                files = []
        inferred = infer_bundle_focus_name(
            files or [node.path],
            fallback_name=node.path.name,
            delimiter=self.config.naming.delimiter,
            max_segment_length=self.config.naming.max_segment_length,
            generic_tokens=(*self.config.generic_tokens, *self.config.managed_root_names()),
        )
        if not inferred:
            return None
        if self.semantic_policy.is_banned_generic_name(inferred):
            return None
        if inferred in self.config.managed_root_names():
            return None
        if inferred in self.config.asset_types:
            return None
        return self._numbered_top_level_name(inferred)

    def _adaptive_name_is_blocked_for_top_level(self, value: str) -> bool:
        normalized = normalize_segment(
            value,
            self.config.naming.delimiter,
            self.config.naming.max_segment_length,
        )
        if not normalized:
            return True
        if normalized in self.config.adaptive_placement.blocked_top_level_names:
            return True
        return any(fragment in normalized for fragment in self.config.adaptive_placement.blocked_top_level_fragments)

    def _rule_based(self, node: IndexedNode) -> ClassificationResult:
        if node.path.name in self.config.ignore_names:
            return ClassificationResult(
                placement_mode="keep_here",
                target_path=None,
                confidence=1.0,
                rationale="ignored system file",
                source="rule",
                review_required=False,
            )

        if node.kind == "dir" and self._is_empty_or_metadata_only_dir(node.path):
            return ClassificationResult(
                placement_mode="keep_here",
                target_path=None,
                confidence=1.0,
                rationale="empty or metadata-only directory is cleanup-only and should not be refiled",
                source="rule",
                review_required=False,
                metadata={"cleanup_only_dir": True},
                stream="system",
                domain="system",
                focus="system",
                asset_type=self.config.naming.misc_asset_type,
            )

        asset_type = self._asset_type_from_node(node)
        review_destination_root = self._review_destination_root_for(node)
        in_hidden_review = review_destination_root == "review_staging"
        adaptive = self._adaptive_rule_based(node, asset_type)
        if adaptive is not None:
            return adaptive

        if node.kind == "dir" and self._is_project_root(node.path):
            domain = self._project_domain(node.path)
            focus = normalize_segment(node.path.name, self.config.naming.delimiter, self.config.naming.max_segment_length)
            if not focus or focus.lower() in {token.lower() for token in self.config.generic_tokens}:
                return self._review_result(
                    confidence=0.45,
                    rationale="project root name is too generic to create a meaningful top-level destination automatically",
                    asset_type="code",
                    destination_root=review_destination_root,
                )
            return ClassificationResult(
                placement_mode="direct",
                target_path=f"projects/{domain}/{focus}",
                confidence=0.95,
                rationale="project root should stay as a single unit inside the typed projects tree",
                source="heuristic",
                review_required=False,
                stream="projects",
                domain=domain,
                focus=focus,
                asset_type="code",
            )

        if node.ext.lower() in INCOMPLETE_SUFFIXES or node.path.name.lower().endswith(INCOMPLETE_SUFFIXES):
            return ClassificationResult(
                placement_mode="keep_here",
                target_path=None,
                confidence=1.0,
                rationale="incomplete download marker is not ready for placement",
                source="rule",
                review_required=False,
                metadata={"gate_state": "incomplete_or_transient"},
            )

        signal_candidates = self._domain_signal_candidates(node, asset_type)
        best_allowed = next((item for item in signal_candidates if self.semantic_policy.is_allowed_domain(item["domain"])), None)
        if best_allowed is not None and asset_type != self.config.naming.misc_asset_type:
            resolved_stream = str(best_allowed.get("stream") or self._default_stream_for_domain(best_allowed["domain"]))
            resolved_focus = (
                str(best_allowed.get("focus"))
                if best_allowed.get("focus")
                else self._suggest_domain_focus(
                    node=node,
                    stream=resolved_stream,
                    domain=str(best_allowed["domain"]),
                    asset_type=asset_type,
                )
            )
            if (
                self._is_under_watch_root(node.path)
                and not _is_relative_to(node.path, self.config.spaces_root)
                and not best_allowed.get("existing_target")
            ):
                return self._review_result(
                    confidence=min(0.74, max(0.6, float(best_allowed["confidence"]) - 0.06)),
                    rationale="approved domain hint exists, but a new final placement still requires review",
                    asset_type=asset_type,
                    focus=self._suggest_review_focus(
                        node=node,
                        asset_type=asset_type,
                        preferred=str(best_allowed["domain"]),
                    ),
                    metadata={
                        "hinted_domain": best_allowed["domain"],
                        "domain_signals": list(best_allowed["signals"]),
                    },
                    destination_root=review_destination_root,
                )
            if in_hidden_review and not best_allowed.get("existing_target"):
                return self._review_result(
                    confidence=min(0.78, max(0.62, float(best_allowed["confidence"]) - 0.04)),
                    rationale="hidden review item has a strong domain hint, but it stays in adaptive review until it matches a proven destination",
                    asset_type=asset_type,
                    focus=self._suggest_review_focus(
                        node=node,
                        asset_type=asset_type,
                        preferred=str(best_allowed["domain"]),
                    ),
                    metadata={
                        "hinted_domain": best_allowed["domain"],
                        "domain_signals": list(best_allowed["signals"]),
                        "hidden_review_grouping": True,
                    },
                    destination_root=review_destination_root,
                )
            target_path = self._target_path_for_domain(
                domain=best_allowed["domain"],
                asset_type=asset_type,
                stream=resolved_stream,
                focus=resolved_focus,
            )
            placement_mode = "merge_existing" if best_allowed.get("existing_target") else "direct"
            if best_allowed.get("existing_target"):
                target_path = str(best_allowed["existing_target"])
            confidence = max(0.82, min(0.97, 0.72 + (0.08 * len(best_allowed["signals"]))))
            return ClassificationResult(
                placement_mode=placement_mode,
                target_path=target_path,
                confidence=confidence,
                rationale="high-signal domain evidence matched an approved filing area",
                source="heuristic",
                review_required=False,
                metadata={
                    "domain_signals": list(best_allowed["signals"]),
                    "hinted_domain": best_allowed["domain"],
                },
                stream=resolved_stream,
                domain=str(best_allowed["domain"]),
                focus=str(resolved_focus or self.config.repair_defaults.fallback_focus),
                asset_type=asset_type,
            )

        best_candidate = signal_candidates[0] if signal_candidates else None
        if best_candidate is not None:
            gate = self.semantic_policy.observe_candidate(
                domain=str(best_candidate["domain"]),
                node_path=node.path,
                confidence=float(best_candidate["confidence"]),
                signals=list(best_candidate["signals"]),
                source=str(best_candidate["source"]),
            )
            gate_metadata = gate.to_dict() if gate is not None else {}
            return self._review_result(
                confidence=min(0.74, max(0.55, float(best_candidate["confidence"]) - 0.08)),
                rationale="candidate domain needs approval before final placement",
                asset_type=asset_type,
                focus=self._suggest_review_focus(
                    node=node,
                    asset_type=asset_type,
                    preferred=str(best_candidate["domain"]),
                ),
                metadata={
                    "candidate_domain": best_candidate["domain"],
                    "candidate_domain_signals": list(best_candidate["signals"]),
                    "candidate_domain_source": best_candidate["source"],
                    "domain_gate": gate_metadata,
                },
                destination_root=review_destination_root,
            )

        if asset_type in REVIEW_MOVABLE_ASSET_TYPES or asset_type == self.config.naming.misc_asset_type:
            return self._review_result(
                confidence=0.55,
                rationale="non-project asset defaults to review until a stronger approved topic path is proven",
                asset_type=asset_type,
                focus=self._suggest_review_focus(node=node, asset_type=asset_type),
                destination_root=review_destination_root,
            )

        return self._review_result(
            confidence=0.35,
            rationale="ambiguous item requires manual review",
            asset_type=asset_type,
            focus=self._suggest_review_focus(node=node, asset_type=asset_type),
            destination_root=review_destination_root,
        )

    def _llm_fallback(self, node: IndexedNode, current: ClassificationResult) -> ClassificationResult:
        allow_cloud = self._allow_cloud_llm_this_batch()
        self._pending_node = node
        self._pending_current = current
        try:
            invocation = self._llm_controller.invoke(
                preferred_provider=self._preferred_provider,
                allow_cloud=allow_cloud,
            )
        except LLMDeferredDecision as exc:
            metadata = {
                **current.metadata,
                "deferred_reason": exc.reason,
                "defer_until": exc.defer_until,
                "provider_attempts": list(exc.provider_attempts),
                "last_error_code": exc.last_error_code,
            }
            return ClassificationResult(
                placement_mode=current.placement_mode,
                target_path=current.target_path,
                create_folders=current.create_folders,
                confidence=current.confidence,
                rationale=current.rationale,
                source=current.source,
                alternatives=current.alternatives,
                review_required=current.review_required,
                metadata=metadata,
                space=current.space,
                stream=current.stream,
                domain=current.domain,
                focus=current.focus,
                asset_type=current.asset_type,
            )

        if invocation.cloud_provider_used and self._batch_command == "watch":
            self._watch_cloud_invocations += 1
        payload = self._sanitize_llm_payload(payload=invocation.payload, current=current)
        payload_metadata = {
            **current.metadata,
            **payload.pop("metadata", {}),
            "provider_used": invocation.provider_used,
            "provider_attempts": list(invocation.provider_attempts),
        }
        result = ClassificationResult(
            placement_mode=str(payload.get("placement_mode", current.placement_mode)),
            target_path=payload.get("target_path"),
            create_folders=tuple(payload.get("create_folders", [])),
            confidence=float(payload.get("confidence", current.confidence)),
            rationale=str(payload.get("reason", current.rationale)),
            source="llm",
            alternatives=tuple(payload.get("alternatives", [])),
            review_required=bool(payload.get("review_required", False)),
            metadata=payload_metadata,
        )
        if result.target_path is None:
            result.space = current.space
            result.stream = current.stream
            result.domain = current.domain
            result.focus = current.focus
            result.asset_type = current.asset_type
        if payload_metadata.get("banned_target_segment") or payload_metadata.get("banned_new_segment"):
            return result
        if self.config.llm.enable_llm_first:
            return result
        return result if result.confidence >= current.confidence else current

    def _allow_cloud_llm_this_batch(self) -> bool:
        if self._batch_command != "watch":
            return True
        return self._watch_cloud_invocations < max(0, int(self.config.llm.max_items_per_watch_tick))

    def _llm_runtime_ready(self) -> bool:
        if self.config.groq.enabled and os.getenv("GROQ_API_KEY"):
            return True
        if os.getenv("GEMINI_API_KEY"):
            return True
        return self._ollama_server_available()

    def _ollama_server_available(self) -> bool:
        if not _OLLAMA_AVAILABLE:
            return False
        base_url = self.config.llm.ollama.base_url.rstrip("/")
        try:
            req = _urllib_request.Request(f"{base_url}/api/tags", method="GET")
            with _urllib_request.urlopen(req, timeout=self.config.llm.ollama.healthcheck_timeout_seconds) as resp:
                return resp.status == 200
        except Exception:
            return False

    def _should_try_llm_fallback(self, node: IndexedNode, current: ClassificationResult) -> bool:
        if node.path.name in self.config.ignore_names or self._is_transient_system_file(node.path):
            return False
        if self.config.is_protected_project_internal(node.path):
            return False
        if node.kind == "dir" and self._is_project_root(node.path):
            return False
        under_watch_root = self._is_under_watch_root(node.path)
        if self._batch_command == "watch":
            return bool(self.config.llm.enable_for_watch) and under_watch_root and "deferred_reason" not in current.metadata
        return under_watch_root and "deferred_reason" not in current.metadata

    def _call_groq(self) -> dict[str, Any]:
        if Groq is None:
            raise LLMProviderError("groq package is not installed", error_code="missing_dependency")
        if not self.config.groq.enabled or not os.getenv("GROQ_API_KEY"):
            raise LLMProviderError("groq is not configured", error_code="not_configured")
        if self._groq_client is None:
            self._groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))
        payload = self._pending_prompt_payload()
        response = None
        try:
            response = self._groq_client.chat.completions.create(
                model=self.config.groq.model,
                messages=[
                    {"role": "system", "content": LLM_CLASSIFIER_SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                response_format={"type": "json_object"},
            )
        except Exception as exc:  # pragma: no cover - network/API dependent
            self._raise_provider_error("groq", exc)
        content = response.choices[0].message.content or "{}"
        return json.loads(content)

    def _call_gemini(self) -> dict[str, Any]:
        if genai is None:
            raise LLMProviderError("google-generativeai package is not installed", error_code="missing_dependency")
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise LLMProviderError("gemini is not configured", error_code="not_configured")
        if self._gemini_client is None:
            genai.configure(api_key=api_key)
            self._gemini_client = genai.GenerativeModel("gemini-2.0-flash")
        payload = self._pending_prompt_payload()
        try:
            response = self._gemini_client.generate_content(
                contents=json.dumps(payload, ensure_ascii=False),
                generation_config={
                    "temperature": 0.1,
                    "response_mime_type": "application/json",
                },
            )
        except Exception as exc:  # pragma: no cover - network/API dependent
            self._raise_provider_error("gemini", exc)
        content = (response.text or "{}").strip()
        return json.loads(content)

    def _call_ollama(self) -> dict[str, Any]:
        if not _OLLAMA_AVAILABLE:
            raise LLMProviderError("urllib is not available for ollama", error_code="missing_dependency")
        payload = self._pending_prompt_payload()
        base_url = self.config.llm.ollama.base_url.rstrip("/")
        request_body = json.dumps(
            {
                "model": self.config.llm.ollama.model,
                "messages": [
                    {"role": "system", "content": LLM_CLASSIFIER_SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                "stream": False,
                "format": "json",
                "options": {"temperature": 0.1},
            },
            ensure_ascii=False,
        ).encode("utf-8")
        req = _urllib_request.Request(
            f"{base_url}/api/chat",
            data=request_body,
            method="POST",
            headers={"Content-Type": "application/json"},
        )
        try:
            with _urllib_request.urlopen(req, timeout=60) as resp:
                raw = resp.read().decode("utf-8")
        except Exception as exc:  # pragma: no cover - network/API dependent
            self._raise_provider_error("ollama", exc)
        payload = json.loads(raw)
        content = payload.get("message", {}).get("content") or "{}"
        return json.loads(content)

    def _raise_provider_error(self, provider: str, exc: Exception) -> None:
        message = str(exc)
        normalized = message.lower()
        if any(token in normalized for token in ("429", "too many requests", "rate limit", "rate_limit")):
            raise LLMRateLimitError(f"{provider} rate limited: {exc}", error_code="rate_limit") from exc
        if "resource_exhausted" in normalized or "quota" in normalized:
            raise LLMRateLimitError(f"{provider} quota exhausted: {exc}", error_code="quota") from exc
        raise LLMProviderError(f"{provider} request failed: {exc}", error_code="provider_error") from exc

    def _pending_prompt_payload(self) -> dict[str, Any]:
        node = getattr(self, "_pending_node", None)
        current = getattr(self, "_pending_current", None)
        if node is None or current is None:
            raise AppError("LLM prompt payload requested without pending classifier state")
        return self._llm_prompt_payload(node=node, current=current)

    def _llm_prompt_payload(self, *, node: IndexedNode, current: ClassificationResult) -> dict[str, Any]:
        self._pending_node = node
        self._pending_current = current
        return {
            "base_dir": "spaces_root",
            "item_path": self._llm_item_path_hint(node.path),
            "filename": node.path.name,
            "extension": node.ext,
            "size": node.size,
            "timestamps": {"mtime": node.mtime, "ctime": node.ctime},
            "content_hints": self._content_hint(node.path),
            "existing_tree_summary": self._existing_tree_summary(),
            "protected_paths": ["projects", "state_dir", "history_root"],
            "current_guess": current.to_dict(),
            "notes": {
                "preferred_provider": self._preferred_provider,
                "watch_command": self._batch_command == "watch",
            },
        }

    def _llm_item_path_hint(self, path: Path) -> str:
        roots: list[tuple[str, Path]] = [("spaces_root", self.config.spaces_root)]
        roots.extend((f"watch_root_{index}", root) for index, root in enumerate(self.config.watch_roots, start=1))
        for label, root in roots:
            if not _is_relative_to(path, root):
                continue
            try:
                relative = path.resolve(strict=False).relative_to(root.resolve(strict=False))
            except ValueError:
                try:
                    relative = path.relative_to(root)
                except ValueError:
                    continue
            relative_value = relative.as_posix() if relative.parts else path.name
            return f"{label}/{relative_value}"
        return path.name

    def _existing_tree_summary(self) -> list[str]:
        summary: list[str] = []
        root = self.config.spaces_root
        if not root.exists():
            return summary
        stack: list[tuple[Path, int]] = [(root, 0)]
        while stack and len(summary) < 60:
            current, depth = stack.pop(0)
            if depth > 3:
                continue
            try:
                children = sorted((child for child in current.iterdir() if child.is_dir()), key=lambda path: path.name.lower())
            except OSError:
                continue
            for child in children:
                try:
                    summary.append(str(child.relative_to(root)))
                except ValueError:
                    continue
                stack.append((child, depth + 1))
                if len(summary) >= 60:
                    break
        return summary

    def _sanitize_llm_payload(self, *, payload: dict[str, Any], current: ClassificationResult) -> dict[str, Any]:
        if not isinstance(payload, dict):
            return current.to_dict()

        placement_mode = str(payload.get("placement_mode", "review_only")).strip().lower()
        if placement_mode not in {"direct", "single_file_folder", "merge_existing", "review_only", "keep_here"}:
            placement_mode = "review_only"

        target_path = self._sanitize_target_path(payload.get("target_path"))
        create_folders = self._sanitize_create_folders(payload.get("create_folders"))
        confidence = _clamp(payload.get("confidence", current.confidence), low=0.0, high=1.0, default=current.confidence)
        reason = str(payload.get("reason") or current.rationale or "llm placement suggestion")
        alternatives = payload.get("alternatives") if isinstance(payload.get("alternatives"), list) else []

        target_depth = len([part for part in (target_path or "").split("/") if part])
        invalid_new_root_count = self._new_top_level_count(target_path=target_path, create_folders=create_folders)
        has_banned_target_segment = self._has_banned_target_segment(target_path)
        has_banned_new_segment = self._has_banned_new_segment(create_folders=create_folders)
        fallback_target = current.target_path
        pending = getattr(self, "_pending_node", None)
        if fallback_target and self._has_banned_target_segment(fallback_target):
            fallback_target = None

        candidate_domain = self._candidate_domain_from_target_path(target_path)
        candidate_gate: dict[str, Any] | None = None
        if candidate_domain and not self.semantic_policy.is_allowed_domain(candidate_domain):
            pending = getattr(self, "_pending_node", None)
            signals = ["llm"]
            if pending is not None and self._domain_keyword_signal(candidate_domain, pending):
                signals.append("filename_keyword")
            gate = self.semantic_policy.observe_candidate(
                domain=candidate_domain,
                node_path=pending.path if pending is not None else Path("."),
                confidence=confidence,
                signals=signals,
                source="llm",
            )
            candidate_gate = gate.to_dict() if gate is not None else None
            placement_mode = "review_only"
            confidence = min(confidence, 0.25)
            target_path = fallback_target or self._review_target_for_asset(current.asset_type)
        elif candidate_domain and self.semantic_policy.is_allowed_domain(candidate_domain) and target_path:
            parts = [part for part in target_path.split("/") if part]
            if parts and parts[0] not in self.config.streams:
                target_path = self._target_path_for_domain(
                    domain=candidate_domain,
                    asset_type=current.asset_type or self.config.naming.misc_asset_type,
                    stream=self._default_stream_for_domain(candidate_domain),
                )

        target_depth = len([part for part in (target_path or "").split("/") if part])
        invalid_new_root_count = self._new_top_level_count(target_path=target_path, create_folders=create_folders)
        has_banned_target_segment = self._has_banned_target_segment(target_path)
        has_banned_new_segment = self._has_banned_new_segment(create_folders=create_folders)

        if (
            self._batch_command == "watch"
            and pending is not None
            and pending.kind == "file"
            and current.metadata.get("adaptive_review")
            and placement_mode in {"direct", "single_file_folder", "merge_existing"}
        ):
            if current.asset_type in {"docs", "slides", "notes", "forms"} and target_depth < 2:
                placement_mode = "review_only"
                confidence = min(confidence, max(0.35, current.confidence))
                target_path = fallback_target or self._review_target_for_asset(current.asset_type)
                target_depth = len([part for part in (target_path or "").split("/") if part])
            elif current.asset_type == "archives":
                placement_mode = "review_only"
                confidence = min(confidence, max(0.4, current.confidence))
                target_path = fallback_target or self._review_target_for_asset(current.asset_type)
                target_depth = len([part for part in (target_path or "").split("/") if part])

        if target_depth >= 5 or invalid_new_root_count > 1 or has_banned_target_segment or has_banned_new_segment:
            placement_mode = "review_only"
            confidence = min(confidence, 0.25)
            if placement_mode == "review_only":
                target_path = fallback_target

        if placement_mode == "review_only" and not target_path:
            target_path = fallback_target or self._review_target_for_asset(current.asset_type)
        if placement_mode in {"direct", "single_file_folder", "merge_existing"} and not target_path:
            placement_mode = "review_only"
            confidence = min(confidence, 0.3)
            target_path = fallback_target or self._review_target_for_asset(current.asset_type)

        review_required = placement_mode == "review_only" or confidence < self.config.groq.confidence_threshold
        return {
            "placement_mode": placement_mode,
            "target_path": target_path,
            "create_folders": create_folders,
            "confidence": confidence,
            "reason": reason,
            "alternatives": alternatives,
            "review_required": review_required,
            "metadata": {
                "llm_mode": placement_mode,
                "llm_target_path": target_path,
                "target_depth": target_depth,
                "new_top_level_count": invalid_new_root_count,
                "banned_target_segment": has_banned_target_segment,
                "banned_new_segment": has_banned_new_segment,
                "candidate_domain": candidate_domain,
                "candidate_domain_gate": candidate_gate,
            },
        }

    def _sanitize_target_path(self, raw_value: Any) -> str | None:
        if raw_value is None:
            return None
        raw = str(raw_value).strip().replace("\\", "/").strip("/")
        if not raw or raw.startswith(".."):
            return None
        parts: list[str] = []
        for part in raw.split("/"):
            if part in {"", ".", ".."}:
                return None
            normalized = normalize_segment(part, self.config.naming.delimiter, self.config.naming.max_segment_length)
            if normalized:
                parts.append(normalized)
        if not parts:
            return None
        return "/".join(parts)

    def _sanitize_create_folders(self, raw_value: Any) -> list[str]:
        if not isinstance(raw_value, list):
            return []
        normalized: list[str] = []
        for item in raw_value[:10]:
            target = self._sanitize_target_path(item)
            if target:
                normalized.append(target)
        return normalized

    def _new_top_level_count(self, *, target_path: str | None, create_folders: list[str]) -> int:
        try:
            existing = {child.name for child in self.config.spaces_root.iterdir() if child.is_dir()}
        except OSError:
            existing = set()
        top_levels: set[str] = set()
        for value in [target_path, *create_folders]:
            if not value:
                continue
            top = value.split("/", 1)[0]
            if top not in existing:
                top_levels.add(top)
        return len(top_levels)

    def _has_banned_new_segment(self, *, create_folders: list[str]) -> bool:
        for folder in create_folders:
            for segment in folder.split("/"):
                if self._is_banned_target_segment_value(segment):
                    return True
        return False

    def _has_banned_target_segment(self, target_path: str | None) -> bool:
        if not target_path:
            return False
        parts = [part for part in target_path.split("/") if part]
        for index, segment in enumerate(parts):
            if parts and parts[0] == "review" and (
                (index == 1 and (segment in self.config.asset_types or segment == self.config.repair_defaults.fallback_focus))
                or (index == len(parts) - 1 and segment in self.config.asset_types)
            ):
                continue
            if self._is_banned_target_segment_value(segment):
                return True
        return False

    def _is_banned_target_segment_value(self, value: str) -> bool:
        return self.semantic_policy.is_banned_generic_name(value)

    def _review_result(
        self,
        confidence: float,
        rationale: str,
        *,
        asset_type: str | None,
        metadata: dict[str, Any] | None = None,
        focus: str | None = None,
        destination_root: str | None = None,
    ) -> ClassificationResult:
        payload_metadata = dict(metadata or {})
        if destination_root:
            payload_metadata["destination_root"] = destination_root
        target_path = self._review_target_for_asset(
            asset_type,
            focus=focus,
            allow_all_assets=destination_root == "review_staging",
        )
        return ClassificationResult(
            placement_mode="review_only",
            target_path=target_path,
            confidence=confidence,
            rationale=rationale,
            source="heuristic",
            review_required=True,
            metadata=payload_metadata,
            asset_type=asset_type or "misc",
        )

    def _review_target_for_asset(
        self,
        asset_type: str | None,
        *,
        focus: str | None = None,
        allow_all_assets: bool = False,
    ) -> str | None:
        resolved_asset = asset_type or self.config.naming.misc_asset_type
        if (
            allow_all_assets
            or resolved_asset in REVIEW_MOVABLE_ASSET_TYPES
            or resolved_asset == self.config.naming.misc_asset_type
        ):
            normalized_focus = normalize_segment(
                focus or self.config.repair_defaults.fallback_focus,
                self.config.naming.delimiter,
                self.config.naming.max_segment_length,
            )
            if self.config.review_mode == "single-inbox" or normalized_focus in {"", self.config.repair_defaults.fallback_focus}:
                return f"review/{resolved_asset}"
            return f"review/{normalized_focus}/{resolved_asset}"
        return None

    def _asset_type_from_node(self, node: IndexedNode) -> str:
        ext = node.ext.lower()
        if node.kind == "dir" and self._is_project_root(node.path):
            return "code"
        if ext in CODE_EXTENSIONS:
            return "code"
        if ext in DOC_EXTENSIONS:
            return "forms" if any(token in node.path.name.lower() for token in FORM_KEYWORDS) else "docs"
        if ext in SLIDE_EXTENSIONS:
            return "slides"
        if ext in NOTE_EXTENSIONS:
            return "notes"
        if ext in DATA_EXTENSIONS:
            return "data"
        if ext in ASSET_EXTENSIONS:
            return "assets"
        if ext in INSTALLER_EXTENSIONS:
            return "installers"
        if ext in ARCHIVE_EXTENSIONS:
            return "archives"
        return self.config.naming.misc_asset_type

    def _project_domain(self, path: Path) -> str:
        text = unicodedata.normalize("NFKC", str(path)).lower()
        for domain, hints in PROJECT_DOMAIN_HINTS.items():
            if any(hint in text for hint in hints):
                return domain
        return "apps"

    def _domain_signal_candidates(self, node: IndexedNode, asset_type: str) -> list[dict[str, Any]]:
        hint_text = self._content_hint(node.path)
        text = unicodedata.normalize("NFKC", f"{node.path}\n{hint_text}").lower()
        candidates: dict[str, dict[str, Any]] = {}
        for override in self.config.pattern_overrides:
            try:
                matched = re.search(override.pattern, text)
            except re.error:
                continue
            if not matched or not override.domain:
                continue
            domain = self.semantic_policy.normalize_domain(override.domain)
            if not domain:
                continue
            entry = candidates.setdefault(
                domain,
                {
                    "domain": domain,
                    "stream": override.stream,
                    "focus": override.focus,
                    "signals": set(),
                    "confidence": 0.82,
                    "source": "pattern",
                    "existing_target": None,
                },
            )
            entry["signals"].add("pattern")
            if self._domain_keyword_signal(domain, node):
                entry["signals"].add("filename_keyword")
                entry["confidence"] = max(entry["confidence"], 0.88)
            if hint_text and self._domain_content_signal(domain, hint_text):
                entry["signals"].add("content_keyword")
                entry["confidence"] = max(entry["confidence"], 0.9)
            if override.asset_type and override.asset_type == asset_type:
                entry["signals"].add("asset_type_hint")
                entry["confidence"] = max(entry["confidence"], 0.9)

        for domain in self.config.allowed_domains():
            if self._domain_keyword_signal(domain, node):
                entry = candidates.setdefault(
                    domain,
                    {
                        "domain": domain,
                        "stream": None,
                        "focus": None,
                        "signals": set(),
                        "confidence": 0.76,
                        "source": "keyword",
                        "existing_target": None,
                    },
                )
                entry["signals"].add("filename_keyword")
            elif hint_text and self._domain_content_signal(domain, hint_text):
                entry = candidates.setdefault(
                    domain,
                    {
                        "domain": domain,
                        "stream": None,
                        "focus": None,
                        "signals": set(),
                        "confidence": 0.78,
                        "source": "content",
                        "existing_target": None,
                    },
                )
                entry["signals"].add("content_keyword")

        for entry in candidates.values():
            existing_target = self._existing_domain_target(
                node=node,
                domain=str(entry["domain"]),
                asset_type=asset_type,
                stream=str(entry.get("stream") or self._default_stream_for_domain(str(entry["domain"]))),
            )
            if existing_target is not None:
                entry["existing_target"] = existing_target
                entry["signals"].add("existing_similarity")
                entry["confidence"] = max(float(entry["confidence"]), 0.9)

        ranked = sorted(
            candidates.values(),
            key=lambda item: (
                1 if self.semantic_policy.is_allowed_domain(str(item["domain"])) else 0,
                len(item["signals"]),
                float(item["confidence"]),
            ),
            reverse=True,
        )
        for item in ranked:
            item["signals"] = tuple(sorted(item["signals"]))
        return ranked

    def _domain_keyword_signal(self, domain: str, node: IndexedNode) -> bool:
        text = unicodedata.normalize("NFKC", f"{node.path.stem}\n{self._content_hint(node.path)}").lower()
        domain_value = self.semantic_policy.normalize_domain(domain)
        if domain_value and domain_value.replace("-", " ") in text:
            return True
        aliases = self.config.domain_aliases.get(domain_value, ())
        return any(alias.lower() in text for alias in aliases)

    def _domain_content_signal(self, domain: str, hint_text: str) -> bool:
        if not hint_text:
            return False
        text = unicodedata.normalize("NFKC", hint_text).lower()
        domain_value = self.semantic_policy.normalize_domain(domain)
        if domain_value and domain_value.replace("-", " ") in text:
            return True
        aliases = self.config.domain_aliases.get(domain_value, ())
        return any(alias.lower() in text for alias in aliases)

    def _default_stream_for_domain(self, domain: str) -> str:
        if domain in {"admin", "finance", "legal"}:
            return "areas"
        return "resources"

    def _target_path_for_domain(
        self,
        *,
        domain: str,
        asset_type: str,
        stream: str,
        focus: str | None = None,
    ) -> str:
        normalized_focus = normalize_segment(
            focus or self.config.repair_defaults.fallback_focus,
            self.config.naming.delimiter,
            self.config.naming.max_segment_length,
        )
        if normalized_focus in {"", domain, self.config.repair_defaults.fallback_focus}:
            return f"{stream}/{domain}/{asset_type}"
        return f"{stream}/{domain}/{normalized_focus}/{asset_type}"

    def _existing_domain_target(self, *, node: IndexedNode, domain: str, asset_type: str, stream: str) -> str | None:
        domain_root = self.config.spaces_root / stream / domain
        if not domain_root.exists() or not domain_root.is_dir():
            return None
        direct_asset = domain_root / asset_type
        if direct_asset.exists() and direct_asset.is_dir():
            return f"{stream}/{domain}/{asset_type}"
        node_tokens = self._semantic_tokens(node.path)
        best_focus: tuple[int, str] | None = None
        try:
            children = sorted((child for child in domain_root.iterdir() if child.is_dir()), key=lambda path: path.name.lower())
        except OSError:
            return None
        for child in children:
            if child.name in self.config.ignore_names or child.name in self.config.asset_types:
                continue
            asset_root = child / asset_type
            if not asset_root.exists() or not asset_root.is_dir():
                continue
            score = len(node_tokens & _path_tokens(child.name))
            if score <= 0:
                continue
            if best_focus is None or score > best_focus[0]:
                best_focus = (score, child.name)
        if best_focus is None:
            return None
        return f"{stream}/{domain}/{best_focus[1]}/{asset_type}"

    def _best_existing_review_focus(self, *, node: IndexedNode, asset_type: str) -> str | None:
        review_root = self.config.spaces_root / "review"
        if not review_root.exists() or not review_root.is_dir():
            return None
        node_tokens = self._semantic_tokens(node.path)
        best_focus: tuple[int, str] | None = None
        try:
            children = sorted((child for child in review_root.iterdir() if child.is_dir()), key=lambda path: path.name.lower())
        except OSError:
            return None
        for child in children:
            if child.name in self.config.ignore_names or child.name in self.config.asset_types:
                continue
            asset_root = child / asset_type
            if not asset_root.exists() or not asset_root.is_dir():
                continue
            score = len(node_tokens & _path_tokens(child.name))
            if score <= 0:
                continue
            if best_focus is None or score > best_focus[0]:
                best_focus = (score, child.name)
        if best_focus is None:
            return None
        return best_focus[1]

    def _candidate_domain_from_target_path(self, target_path: str | None) -> str | None:
        if not target_path:
            return None
        parts = [part for part in target_path.split("/") if part]
        if not parts:
            return None
        head = parts[0]
        if head == "review":
            return None
        if head in {"areas", "resources", "archive", "projects"}:
            if len(parts) >= 2:
                return self.semantic_policy.normalize_domain(parts[1])
            return None
        if head in self.config.streams or head == "system":
            return None
        if self.config.adaptive_mode_enabled():
            return None
        return self.semantic_policy.normalize_domain(head)

    def _is_project_root(self, path: Path) -> bool:
        if not path.is_dir():
            return False
        try:
            child_names = {child.name for child in path.iterdir()}
        except OSError:
            return False
        if child_names & set(self.config.project_markers):
            return True
        if child_names & PROJECT_CONFIG_HINTS:
            return True
        structure_hits = len(child_names & PROJECT_STRUCTURE_HINTS)
        code_hits = 0
        try:
            for child in path.iterdir():
                if child.name in self.config.ignore_names:
                    continue
                if child.is_file() and child.suffix.lower() in CODE_EXTENSIONS:
                    code_hits += 1
        except OSError:
            return False
        if structure_hits >= 2 and code_hits >= 1:
            return True
        if structure_hits >= 1 and code_hits >= 2:
            return True
        try:
            for child in path.rglob("*"):
                if child.name in self.config.ignore_names:
                    continue
                if child.is_dir() and child.name in PROJECT_STRONG_DIR_HINTS:
                    return True
                if child.is_dir() and child.name in PROJECT_STRUCTURE_HINTS:
                    structure_hits += 1
                if child.is_file() and child.name in PROJECT_CONFIG_HINTS:
                    return True
                if child.is_file() and child.suffix.lower() in CODE_EXTENSIONS:
                    code_hits += 1
                if code_hits >= 5:
                    return True
                if structure_hits >= 2 and code_hits >= 2:
                    return True
                if structure_hits >= 1 and code_hits >= 3:
                    return True
        except OSError:
            return False
        return False

    def _is_transient_system_file(self, path: Path) -> bool:
        return any(path.name.startswith(prefix) for prefix in TRANSIENT_SYSTEM_PREFIXES)

    def _is_system_dependency(self, path: Path) -> bool:
        return any(part in SYSTEM_DEPENDENCY_NAMES for part in path.parts)

    def _is_empty_or_metadata_only_dir(self, path: Path) -> bool:
        if not path.is_dir():
            return False
        try:
            children = sorted(path.iterdir(), key=lambda child: child.name.lower())
        except OSError:
            return False
        if not children:
            return True
        for child in children:
            if child.name in self.config.ignore_names or METADATA_ARTIFACT_PATTERN.search(child.name):
                continue
            if child.is_symlink():
                return False
            if child.is_dir():
                if self._is_empty_or_metadata_only_dir(child):
                    continue
                return False
            return False
        return True

    def _is_under_watch_root(self, path: Path) -> bool:
        return any(_is_relative_to(path, root) for root in self.config.watch_roots)


def _is_relative_to(path: Path, root: Path) -> bool:
    candidates = ((path, root), (path.resolve(strict=False), root.resolve(strict=False)))
    for candidate_path, candidate_root in candidates:
        try:
            candidate_path.relative_to(candidate_root)
            return True
        except ValueError:
            continue
    return False


def _clamp(value: Any, *, low: float, high: float, default: float) -> float:
    try:
        converted = float(value)
    except (TypeError, ValueError):
        converted = default
    return max(low, min(high, converted))


def _path_tokens(value: str) -> set[str]:
    text = unicodedata.normalize("NFKC", str(value)).lower().replace("-", " ")
    return {token for token in re.findall(r"[0-9a-z가-힣]+", text) if len(token) >= 2}


def _adaptive_tokens_for_name(value: str) -> set[str]:
    tokens = _path_tokens(value)
    compact = _normalized_archive_match_text(value)
    if compact:
        tokens.add(compact)
        tokens.update(_path_tokens(compact))
    return {token for token in tokens if token}


def _fuzzy_token_overlap_score(source_tokens: set[str], candidate_tokens: set[str]) -> int:
    if not source_tokens or not candidate_tokens:
        return 0
    score = 0
    for source in source_tokens:
        for candidate in candidate_tokens:
            if _is_short_numeric_token(source) or _is_short_numeric_token(candidate):
                continue
            if source == candidate:
                score += 2
                continue
            if len(source) >= 2 and len(candidate) >= 2 and (source in candidate or candidate in source):
                score += 1
    return score


def _is_short_numeric_token(token: str) -> bool:
    text = str(token).strip()
    return text.isdigit() and len(text) <= 4


def _numbered_prefix_value(value: str, *, width: int) -> int:
    pattern = NUMBERED_TOP_LEVEL_PATTERN if width == 3 else NUMBERED_SUBTOPIC_PATTERN
    match = pattern.match(str(value).strip())
    if not match:
        return 0
    try:
        return int(match.group("number"))
    except (TypeError, ValueError):
        return 0


def _topic_hint_score(*, source_text: str, target_name: str, asset_type: str) -> int:
    target_text = _normalized_archive_match_text(target_name)
    if not target_text:
        return 0
    score = 0
    if asset_type == "archives" and any(token in target_text for token in ("압축", "archive")):
        score += 3
    if asset_type == "data" and any(token in target_text for token in ("데이터", "분석", "analysis")):
        score += 2
    if asset_type == "code" and any(token in target_text for token in ("project", "프로젝트", "개발")):
        score += 2
    if any(token in source_text for token in ("신청서", "양식", "form")) and any(token in target_text for token in ("양식", "법무", "제안")):
        score += 2
    if any(token in source_text for token in ("제안서", "proposal")) and "제안" in target_text:
        score += 2
    if any(token in source_text for token in ("교육", "강의", "학습")) and any(token in target_text for token in ("교육", "학습")):
        score += 2
    if any(token in source_text for token in ("서비스", "정의서", "spec")) and any(token in target_text for token in ("서비스", "정의")):
        score += 2
    if any(token in source_text for token in ("오디오", "발표", "audio")) and any(token in target_text for token in ("오디오", "발표", "audio")):
        score += 2
    if (
        asset_type in {"assets", "slides"}
        and any(token in source_text for token in ("사진", "photo", "image"))
        and any(token in target_text for token in ("사진", "photo", "image"))
    ):
        score += 2
    return score


def _document_group_label(*, source_text: str, asset_type: str) -> str | None:
    if any(token in source_text for token in ("신청서", "양식", "form")):
        return "신청서"
    if any(token in source_text for token in ("제안서", "proposal")):
        return "제안서"
    if any(token in source_text for token in ("정의서", "spec")):
        return "정의서"
    if any(token in source_text for token in ("회의록", "meeting")):
        return "회의록"
    if any(token in source_text for token in ("보고서", "report")):
        return "보고서"
    if asset_type == "archives":
        return "압축원본"
    if asset_type == "data":
        return "데이터"
    if asset_type == "code":
        return "프로젝트"
    return None


def _extract_period_token(source_text: str) -> str | None:
    match = PERIOD_TOKEN_PATTERN.search(source_text)
    if not match:
        return None
    year = match.group("year")
    month = match.group("month")
    if not year:
        return None
    if month:
        return f"{year}-{month}"
    return year


def _looks_like_temp_bucket(value: str) -> bool:
    text = unicodedata.normalize("NFKC", str(value)).lower()
    return any(marker in text for marker in TEMP_BUCKET_MARKERS)


def _looks_like_temp_source(path: Path) -> bool:
    text = unicodedata.normalize("NFKC", str(path)).lower()
    return any(marker in text for marker in ("tmp", "temp", "임시", "cache"))


def _iter_dir_children(path: Path) -> list[Path]:
    try:
        return [child for child in path.iterdir() if child.is_dir()]
    except OSError:
        return []


def _looks_like_project_collection_root(path: Path, project_markers: tuple[str, ...]) -> bool:
    normalized = unicodedata.normalize("NFKC", path.name).lower()
    if any(marker in normalized for marker in ("project", "projects", "workspace", "workspaces")):
        return True
    children = _iter_dir_children(path)
    if not children:
        return False
    detected = 0
    marker_set = set(project_markers)
    for child in children[:40]:
        try:
            names = {entry.name for entry in child.iterdir()}
        except OSError:
            continue
        if names & marker_set:
            detected += 1
            if detected >= 2:
                return True
            continue
        structure_hits = len(names & PROJECT_STRUCTURE_HINTS)
        code_like = 0
        try:
            for entry in child.iterdir():
                if entry.is_file() and entry.suffix.lower() in CODE_EXTENSIONS:
                    code_like += 1
                if entry.is_dir() and entry.name in PROJECT_STRONG_DIR_HINTS:
                    code_like += 1
        except OSError:
            continue
        if structure_hits >= 2 and code_like >= 1:
            detected += 1
        if detected >= 2:
            return True
    return False


def _normalized_archive_match_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", str(value)).lower()
    compact = re.sub(r"[^0-9a-z가-힣]", "", normalized)
    for marker in ARCHIVE_BUCKET_MARKERS:
        compact = compact.replace(marker, "")
    compact = re.sub(r"\d+차", "", compact)
    return compact


def _archive_candidate_score(*, candidate_name: str, node_text: str) -> int:
    if not node_text:
        return 0
    score = 0
    for token in _path_tokens(candidate_name):
        if token and token in node_text:
            score += 1
    return score


def _looks_like_archive_bucket_name(value: str) -> bool:
    normalized = unicodedata.normalize("NFKC", str(value)).lower()
    compact = re.sub(r"[^0-9a-z가-힣]", "", normalized)
    return any(marker in compact for marker in ARCHIVE_BUCKET_MARKERS)
