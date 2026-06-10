import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from threads_coupang_pipeline.media import (  # noqa: E402
    get_best_image_candidate,
    get_best_video_url,
)


class MediaHelperTests(unittest.TestCase):
    def test_get_best_image_candidate_uses_largest_area(self) -> None:
        candidates = [
            {"url": "small.jpg", "width": 640, "height": 480},
            {"url": "large.jpg", "width": 1200, "height": 900},
            {"url": "medium.jpg", "width": 1000, "height": 600},
        ]

        self.assertEqual(get_best_image_candidate(candidates)["url"], "large.jpg")
        self.assertEqual(get_best_image_candidate(candidates)["_candidate_index"], 1)

    def test_get_best_image_candidate_ignores_invalid_entries(self) -> None:
        candidates = [
            None,
            {"width": 1200, "height": 900},
            {"url": "valid.jpg", "width": 100, "height": 100},
        ]

        self.assertEqual(get_best_image_candidate(candidates)["url"], "valid.jpg")

    def test_get_best_image_candidate_empty(self) -> None:
        self.assertEqual(get_best_image_candidate([]), {})

    def test_get_best_video_url_uses_largest_area(self) -> None:
        self.assertEqual(
            get_best_video_url([
                {"url": "video-small.mp4", "width": 640, "height": 360},
                {"url": "video-large.mp4", "width": 1920, "height": 1080},
                {"url": "video-medium.mp4", "width": 1280, "height": 720},
            ]),
            "video-large.mp4",
        )

    def test_get_best_video_url_returns_first_url_without_dimensions(self) -> None:
        self.assertEqual(
            get_best_video_url([{"url": "video-1.mp4"}, {"url": "video-2.mp4"}]),
            "video-1.mp4",
        )

    def test_get_best_video_url_empty(self) -> None:
        self.assertEqual(get_best_video_url([]), "")


if __name__ == "__main__":
    unittest.main()
