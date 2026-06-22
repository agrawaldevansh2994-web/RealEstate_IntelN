"""
models/ai_explainer.py
Adds Azure OpenAI explanations to suspicious_flags.

This is intentionally a presentation/reporting layer. The core detector logic
stays rule-based and auditable; the LLM only translates evidence into plain
language for buyers, dashboards, and reports.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from openai import AzureOpenAI

from db.connection import select_rows, update_rows

load_dotenv()
logger = logging.getLogger(__name__)

AI_EXPLANATION_KEY = "ai_explanation"


class AIFlagExplainer:
    def __init__(self, city_id: int, limit: int = 10, overwrite: bool = False):
        self.city_id = city_id
        self.limit = limit
        self.overwrite = overwrite
        self.deployment = self._required_env("AZURE_OPENAI_DEPLOYMENT")
        self.client = AzureOpenAI(
            azure_endpoint=self._required_env("AZURE_OPENAI_ENDPOINT"),
            api_key=self._required_env("AZURE_OPENAI_API_KEY"),
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-02-01"),
        )

    def run(self) -> int:
        flags = self._load_candidate_flags()
        if not flags:
            logger.info("AIFlagExplainer: no flags need AI explanations")
            return 0

        updated = 0
        for flag in flags:
            try:
                explanation = self.explain_flag(flag)
                evidence = self._evidence_dict(flag)
                evidence[AI_EXPLANATION_KEY] = explanation
                update_rows(
                    "suspicious_flags",
                    {"id": str(flag["id"])},
                    {"evidence": evidence},
                )
                updated += 1
                logger.info(
                    "AIFlagExplainer: explained %s (%s)",
                    flag.get("id"),
                    flag.get("flag_type"),
                )
            except Exception as exc:
                logger.warning(
                    "AIFlagExplainer: could not explain flag %s: %s",
                    flag.get("id"),
                    exc,
                )

        logger.info("AIFlagExplainer: updated %s flag(s)", updated)
        return updated

    def explain_flag(self, flag: dict[str, Any]) -> dict[str, Any]:
        response = self.client.chat.completions.create(
            model=self.deployment,
            temperature=0.2,
            messages=self._messages_for_flag(flag),
        )
        content = response.choices[0].message.content or "{}"
        explanation = self._parse_json_object(content)
        explanation.setdefault("summary", content.strip())
        explanation.setdefault("buyer_risk", "")
        explanation.setdefault("evidence_used", [])
        explanation.setdefault("recommended_next_step", "")
        explanation["generated_at"] = datetime.now(timezone.utc).isoformat()
        explanation["generated_by"] = "azure_openai"
        explanation["model"] = self.deployment
        return explanation

    def _load_candidate_flags(self) -> list[dict[str, Any]]:
        rows = select_rows(
            "suspicious_flags",
            filters={"city_id": self.city_id},
            limit=5000,
        )
        candidates = []
        for flag in rows:
            if not self._is_open_flag(flag):
                continue

            evidence = self._evidence_dict(flag)
            if not self.overwrite and evidence.get(AI_EXPLANATION_KEY):
                continue

            candidates.append(flag)
            if len(candidates) >= self.limit:
                break

        return candidates

    def _messages_for_flag(self, flag: dict[str, Any]) -> list[dict[str, str]]:
        payload = {
            "flag_type": flag.get("flag_type"),
            "severity": flag.get("severity"),
            "title": flag.get("title"),
            "description": flag.get("description"),
            "confidence": flag.get("confidence"),
            "confidence_note": flag.get("confidence_note"),
            "evidence": self._safe_evidence_for_prompt(flag),
        }
        return [
            {
                "role": "system",
                "content": (
                    "You are a cautious real estate risk analyst for Maharashtra. "
                    "Explain risk signals in simple buyer-facing language. "
                    "Do not accuse anyone of fraud or wrongdoing. Use phrases like "
                    "'risk signal', 'data inconsistency', and 'requires verification'. "
                    "Return only valid JSON with keys: summary, buyer_risk, "
                    "evidence_used, recommended_next_step."
                ),
            },
            {
                "role": "user",
                "content": json.dumps(payload, ensure_ascii=True, default=str),
            },
        ]

    def _safe_evidence_for_prompt(self, flag: dict[str, Any]) -> dict[str, Any]:
        evidence = self._evidence_dict(flag)
        evidence.pop(AI_EXPLANATION_KEY, None)
        return evidence

    @staticmethod
    def _required_env(key: str) -> str:
        value = os.getenv(key)
        if not value:
            raise RuntimeError(f"Missing required environment variable: {key}")
        return value

    @staticmethod
    def _is_open_flag(flag: dict[str, Any]) -> bool:
        status = str(flag.get("status") or "").strip().lower()
        return status in ("", "open")

    @staticmethod
    def _evidence_dict(flag: dict[str, Any]) -> dict[str, Any]:
        evidence = flag.get("evidence") or {}
        if isinstance(evidence, dict):
            return dict(evidence)
        if isinstance(evidence, str):
            try:
                parsed = json.loads(evidence)
                return parsed if isinstance(parsed, dict) else {}
            except (TypeError, ValueError, json.JSONDecodeError):
                return {}
        return {}

    @staticmethod
    def _parse_json_object(content: str) -> dict[str, Any]:
        try:
            parsed = json.loads(content)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            match = re.search(r"\{.*\}", content, flags=re.S)
            if not match:
                return {}
            try:
                parsed = json.loads(match.group(0))
                return parsed if isinstance(parsed, dict) else {}
            except json.JSONDecodeError:
                return {}
