import json
import unittest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from models.ai_explainer import AIFlagExplainer, AI_EXPLANATION_KEY


class AIFlagExplainerTests(unittest.TestCase):
    def setUp(self):
        env = {
            "AZURE_OPENAI_ENDPOINT": "https://example.openai.azure.com/",
            "AZURE_OPENAI_API_KEY": "test-key",
            "AZURE_OPENAI_DEPLOYMENT": "gpt-4o",
            "AZURE_OPENAI_API_VERSION": "2024-02-01",
        }
        patcher_env = patch.dict("os.environ", env)
        patcher_client = patch("models.ai_explainer.AzureOpenAI")
        self.addCleanup(patcher_env.stop)
        self.addCleanup(patcher_client.stop)
        patcher_env.start()
        self.mock_client_class = patcher_client.start()

    def test_load_candidate_flags_skips_closed_and_existing_explanations(self):
        rows = [
            {"id": "f1", "status": "open", "evidence": {}},
            {"id": "f2", "status": "closed", "evidence": {}},
            {"id": "f3", "status": "open", "evidence": {AI_EXPLANATION_KEY: {"summary": "old"}}},
            {"id": "f4", "status": "", "evidence": "{}"},
        ]

        with patch("models.ai_explainer.select_rows", return_value=rows):
            explainer = AIFlagExplainer(city_id=1, limit=10)
            candidates = explainer._load_candidate_flags()

        self.assertEqual([flag["id"] for flag in candidates], ["f1", "f4"])

    def test_explain_flag_adds_metadata_and_omits_existing_ai_summary_from_prompt(self):
        response_content = json.dumps({
            "summary": "This is a risk signal.",
            "buyer_risk": "Verify before purchase.",
            "evidence_used": ["price_per_sqft"],
            "recommended_next_step": "Check source documents.",
        })
        fake_client = MagicMock()
        fake_client.chat.completions.create.return_value = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content=response_content))]
        )

        explainer = AIFlagExplainer(city_id=1)
        explainer.client = fake_client
        flag = {
            "id": "f1",
            "flag_type": "listing_price_outlier",
            "severity": "medium",
            "title": "Listing price outlier in Kaulkhed",
            "description": "Possible data error.",
            "evidence": {
                "price_per_sqft": 85,
                AI_EXPLANATION_KEY: {"summary": "old"},
            },
        }

        explanation = explainer.explain_flag(flag)

        self.assertEqual(explanation["summary"], "This is a risk signal.")
        self.assertEqual(explanation["generated_by"], "azure_openai")
        self.assertEqual(explanation["model"], "gpt-4o")

        messages = fake_client.chat.completions.create.call_args.kwargs["messages"]
        user_payload = json.loads(messages[1]["content"])
        self.assertNotIn(AI_EXPLANATION_KEY, user_payload["evidence"])
        self.assertEqual(user_payload["evidence"]["price_per_sqft"], 85)


if __name__ == "__main__":
    unittest.main()
