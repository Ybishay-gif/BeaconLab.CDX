#!/usr/bin/env python3
import argparse
import glob
import os
from pathlib import Path

import imageio.v2 as imageio
import numpy as np
from PIL import Image, ImageDraw, ImageFont


CAPTIONS = {
    "01_landing": "Open the app",
    "02_login_form_filled": "Enter admin access code",
    "03_after_login": "Access the planning workspace",
    "04_plan_details_filled": "Fill plan name and description",
    "05_plan_created": "Create the plan",
    "06_parameter_filled": "Set a parameter",
    "07_parameter_saved": "Save the parameter",
    "08_decision_filled": "Prepare a decision",
    "09_decision_added": "Add the decision",
    "10_run_queued": "Run the plan",
}


def fit_frame(img: Image.Image, width: int, height: int) -> Image.Image:
    canvas = Image.new("RGB", (width, height), (14, 18, 24))
    src_w, src_h = img.size
    scale = min(width / src_w, height / src_h)
    new_w = max(1, int(src_w * scale))
    new_h = max(1, int(src_h * scale))
    resized = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
    x = (width - new_w) // 2
    y = (height - new_h) // 2
    canvas.paste(resized, (x, y))
    return canvas


def draw_caption(img: Image.Image, text: str) -> Image.Image:
    out = img.copy()
    draw = ImageDraw.Draw(out)
    font = ImageFont.load_default()
    pad = 12
    box_h = 48
    w, h = out.size
    draw.rectangle([(0, h - box_h), (w, h)], fill=(0, 0, 0, 180))
    draw.text((pad, h - box_h + 16), text, fill=(255, 255, 255), font=font)
    return out


def build_timeline(images, fps):
    hold_frames = int(2.4 * fps)
    fade_frames = int(0.6 * fps)
    timeline = []

    for i, frame in enumerate(images):
        frame_np = np.array(frame)
        for _ in range(hold_frames):
            timeline.append(frame_np)

        if i < len(images) - 1:
            next_np = np.array(images[i + 1])
            for j in range(1, fade_frames + 1):
                alpha = j / (fade_frames + 1)
                blended = (frame_np * (1 - alpha) + next_np * alpha).astype(np.uint8)
                timeline.append(blended)

    return timeline


def main():
    parser = argparse.ArgumentParser(description="Render training mp4 from screenshot captures")
    parser.add_argument("--captures-dir", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--fps", type=int, default=12)
    parser.add_argument("--width", type=int, default=1280)
    parser.add_argument("--height", type=int, default=720)
    args = parser.parse_args()

    captures = sorted(glob.glob(os.path.join(args.captures_dir, "*.png")))
    if not captures:
        raise SystemExit(f"No .png captures found in {args.captures_dir}")

    slides = []
    for path in captures:
        name = Path(path).stem
        caption = CAPTIONS.get(name, name.replace("_", " ").title())
        img = Image.open(path).convert("RGB")
        slide = fit_frame(img, args.width, args.height)
        slide = draw_caption(slide, caption)
        slides.append(slide)

    timeline = build_timeline(slides, args.fps)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    writer = imageio.get_writer(
        output_path.as_posix(),
        fps=args.fps,
        codec="libx264",
        quality=8,
        macro_block_size=None,
    )
    for frame in timeline:
        writer.append_data(frame)
    writer.close()

    print(f"Created video: {output_path}")
    print(f"Frames used: {len(captures)}")


if __name__ == "__main__":
    main()
