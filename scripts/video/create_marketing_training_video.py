#!/usr/bin/env python3
import argparse
import json
import math
import os
import subprocess
import urllib.request
from pathlib import Path

import imageio.v2 as imageio
import imageio_ffmpeg
import numpy as np
from PIL import Image, ImageDraw, ImageFont


def choose_font(size):
  try:
    return ImageFont.truetype("/System/Library/Fonts/Supplemental/Arial Bold.ttf", size)
  except Exception:
    return ImageFont.load_default()


def gradient_bg(width, height, top=(8, 28, 54), bottom=(5, 11, 23)):
  arr = np.zeros((height, width, 3), dtype=np.uint8)
  for y in range(height):
    t = y / max(1, height - 1)
    arr[y, :, 0] = int(top[0] * (1 - t) + bottom[0] * t)
    arr[y, :, 1] = int(top[1] * (1 - t) + bottom[1] * t)
    arr[y, :, 2] = int(top[2] * (1 - t) + bottom[2] * t)
  return Image.fromarray(arr, "RGB")


def fit_cover(img, width, height, zoom=1.0):
  src_w, src_h = img.size
  target_ratio = width / height
  src_ratio = src_w / src_h
  if src_ratio > target_ratio:
    crop_h = src_h
    crop_w = int(crop_h * target_ratio / zoom)
  else:
    crop_w = src_w
    crop_h = int(crop_w / target_ratio / zoom)
  crop_w = max(1, min(crop_w, src_w))
  crop_h = max(1, min(crop_h, src_h))
  left = (src_w - crop_w) // 2
  top = (src_h - crop_h) // 2
  cropped = img.crop((left, top, left + crop_w, top + crop_h))
  return cropped.resize((width, height), Image.Resampling.LANCZOS)


def draw_text_overlay(frame, title, subtitle):
  draw = ImageDraw.Draw(frame)
  w, h = frame.size
  draw.rectangle([(0, 0), (w, 130)], fill=(0, 0, 0, 150))
  draw.rectangle([(0, h - 120), (w, h)], fill=(0, 0, 0, 165))
  title_font = choose_font(42)
  sub_font = choose_font(28)
  draw.text((40, 34), title, fill=(255, 255, 255), font=title_font)
  draw.text((40, h - 84), subtitle, fill=(220, 235, 255), font=sub_font)
  return frame


def make_title_card(width, height, title, subtitle):
  frame = gradient_bg(width, height, top=(5, 37, 84), bottom=(7, 17, 36))
  draw = ImageDraw.Draw(frame)
  draw.rounded_rectangle([(70, 120), (width - 70, height - 120)], radius=36, fill=(255, 255, 255, 15), outline=(94, 169, 255), width=2)
  title_font = choose_font(58)
  sub_font = choose_font(30)
  draw.text((120, 220), title, fill=(255, 255, 255), font=title_font)
  draw.text((120, 320), subtitle, fill=(209, 227, 255), font=sub_font)
  return frame


def make_story_frame(width, height, title, subtitle, t, index):
  frame = gradient_bg(width, height, top=(4, 31, 72), bottom=(7, 15, 30))
  draw = ImageDraw.Draw(frame)
  w, h = frame.size

  # Animated ambient shapes for a modern marketing look.
  phase = t * math.pi * 2
  x1 = int(w * 0.18 + math.sin(phase + index * 0.7) * 60)
  y1 = int(h * 0.32 + math.cos(phase * 0.8) * 44)
  x2 = int(w * 0.78 + math.cos(phase + index) * 70)
  y2 = int(h * 0.72 + math.sin(phase * 0.9) * 40)
  draw.ellipse([(x1 - 180, y1 - 180), (x1 + 180, y1 + 180)], fill=(26, 84, 168))
  draw.ellipse([(x2 - 150, y2 - 150), (x2 + 150, y2 + 150)], fill=(14, 116, 167))

  panel_x0 = 80
  panel_y0 = 110
  panel_x1 = w - 80
  panel_y1 = h - 110
  draw.rounded_rectangle(
    [(panel_x0, panel_y0), (panel_x1, panel_y1)],
    radius=42,
    fill=(3, 10, 24, 210),
    outline=(72, 158, 255),
    width=2,
  )

  title_font = choose_font(56)
  subtitle_font = choose_font(30)
  draw.text((140, 220), title, fill=(255, 255, 255), font=title_font)
  draw.text((140, 330), subtitle, fill=(205, 228, 255), font=subtitle_font)
  draw.text((140, 470), "Beacon Lab | Strategy Intelligence Platform", fill=(145, 185, 245), font=choose_font(24))
  return frame


def words_to_seconds(text):
  words = max(1, len(text.split()))
  return max(5.0, words / 2.6 + 1.4)


def build_scenes(captures_dir):
  c = lambda name: captures_dir / f"{name}.png"
  return [
    {
      "kind": "story",
      "title": "The Challenge",
      "subtitle": "Growth teams must make high-stakes pricing decisions with fragmented signals.",
      "narration": "Every growth team faces the same challenge: critical pricing decisions, fragmented signals, and constant pressure to scale without sacrificing efficiency.",
    },
    {
      "kind": "story",
      "title": "The Opportunity",
      "subtitle": "What if every strategy could be tested before budget is committed?",
      "narration": "The opportunity is simple: test strategy before execution. Validate outcomes before committing budget. Scale only what is expected to perform.",
    },
    {
      "kind": "story",
      "title": "The Value Proposition",
      "subtitle": "Beacon Lab turns experimentation data into clear, state-level strategy decisions.",
      "narration": "Beacon Lab turns continuous experimentation data into clear recommendations by state, segment, and channel so teams can move faster with confidence.",
    },
    {
      "kind": "title",
      "title": "From Strategy To Decision",
      "subtitle": "Build plans, simulate scenarios, and choose the strongest path.",
      "narration": "Now let us walk through how Beacon Lab transforms your strategy into measurable outcomes.",
    },
    {
      "image": c("03_plan_builder_home"),
      "title": "What Is A Plan?",
      "subtitle": "A plan combines assumptions, targets, and strategic decisions in one workspace",
      "narration": "In Beacon Lab, a plan is your decision framework. It combines your business assumptions, target metrics, and execution rules so every scenario can be tested consistently.",
    },
    {
      "image": c("04_plan_definition"),
      "title": "Create The Plan Context",
      "subtitle": "Name it, define the objective, and lock the timeframe",
      "narration": "Start by creating a plan with a clear objective. This gives your team a shared context for experimentation and makes every output traceable to the same planning goal.",
    },
    {
      "image": c("06_plan_components_parameter"),
      "title": "Compose Inputs",
      "subtitle": "Set budget, business constraints, and model parameters",
      "narration": "Next, set core parameters like target budget and constraints. These values become the model inputs that shape all projections and optimization recommendations.",
    },
    {
      "image": c("07_plan_components_decision"),
      "title": "Test What-If Scenarios",
      "subtitle": "Add channel and state-level decisions to compare strategy paths",
      "narration": "You can add what-if decisions by state, segment, and channel. This is where scenario planning happens, allowing you to compare multiple growth paths before execution.",
    },
    {
      "image": c("09_strategy_rule_configured"),
      "title": "Build Strategy Rules",
      "subtitle": "Define state and segment coverage for each rule",
      "narration": "In Plan Strategy, create rules that map your intent to specific states and segments. This creates precise control instead of one-size-fits-all bidding behavior.",
    },
    {
      "image": c("11_strategy_cor_target_set"),
      "title": "Set Target COR Per Rule",
      "subtitle": "Tune uplift limits and target combined ratio",
      "narration": "For each rule, set performance guardrails like maximum uplifts and target combined ratio. These constraints keep recommendations aligned with profitability goals.",
    },
    {
      "image": c("13_price_decision_overview"),
      "title": "Price Exploration Decisions",
      "subtitle": "Recommendation engine selects testing points with evidence",
      "narration": "Price Exploration Decisions translates strategy into action. It selects testing points and shows expected impact on win rate, cost, binds, and efficiency metrics.",
    },
    {
      "image": c("14_price_exploration_table"),
      "title": "Thousands Of Ongoing A/B Tests",
      "subtitle": "Continuous experiments discover ideal pricing by state and channel",
      "narration": "Behind the scenes, thousands of ongoing experiments feed this table. The platform continuously learns the ideal pricing level for each state and channel combination.",
    },
    {
      "image": c("15_state_plan_analysis_map"),
      "title": "State Plan Analysis Map",
      "subtitle": "Visualize strategic impact across the U.S.",
      "narration": "State Plan Analysis converts all model outputs into a national view, so teams can instantly see where strategy is aggressive, balanced, or efficiency-focused.",
    },
    {
      "image": c("16_state_tooltip_full"),
      "title": "Hover Tooltips Explain Outcomes",
      "subtitle": "Spend, COR, ROE, binds, uplift, and incremental opportunity",
      "narration": "Hovering any state reveals the full story: spend, combined ratio, ROE, binds, uplift, and estimated incremental opportunity. This supports fast, data-driven decisions.",
    },
    {
      "image": c("17_state_tooltip_zoom"),
      "title": "Model-Driven Recommendation Logic",
      "subtitle": "Each strategy selects relevant testing points and predicts expected outcomes",
      "narration": "The data models select the most relevant testing points for each strategy rule and forecast expected outcomes accordingly, turning experimentation data into clear actions.",
    },
    {
      "image": c("18_plan_run_queued"),
      "title": "Activate And Compare",
      "subtitle": "Run the plan, compare scenarios, and choose the strongest option",
      "narration": "When ready, run the plan to generate scenario outputs and compare alternatives side by side. This enables confident prioritization before budget is deployed.",
    },
    {
      "kind": "title",
      "title": "Business Impact Summary",
      "subtitle": "Faster strategy cycles, smarter pricing, and better capital allocation",
      "narration": "In summary, Beacon Lab helps teams move from intuition to evidence. You can test faster, price smarter, and select the plan with the highest expected business impact.",
    },
  ]


def render_video(scenes, fps, width, height, output_path):
  fade_seconds = 0.6
  fade_frames = max(1, int(fade_seconds * fps))
  frames = []

  for idx, scene in enumerate(scenes):
    duration = words_to_seconds(scene["narration"])
    hold_frames = max(1, int(duration * fps))

    if scene.get("kind") == "title":
      base = make_title_card(width, height, scene["title"], scene["subtitle"])
      scene_frames = [np.array(base)] * hold_frames
    elif scene.get("kind") == "story":
      scene_frames = []
      for i in range(hold_frames):
        t = i / max(1, hold_frames - 1)
        frame = make_story_frame(width, height, scene["title"], scene["subtitle"], t, idx)
        scene_frames.append(np.array(frame))
    else:
      image_path = scene["image"]
      if image_path.exists():
        src = Image.open(image_path).convert("RGB")
      else:
        src = gradient_bg(width, height)
      scene_frames = []
      for i in range(hold_frames):
        t = i / max(1, hold_frames - 1)
        zoom = 1.0 + 0.05 * t
        frame = fit_cover(src, width, height, zoom=zoom)
        frame = draw_text_overlay(frame, scene["title"], scene["subtitle"])
        scene_frames.append(np.array(frame))

    if idx > 0:
      prev = frames[-1]
      first = scene_frames[0]
      for j in range(1, fade_frames + 1):
        a = j / (fade_frames + 1)
        blend = (prev * (1 - a) + first * a).astype(np.uint8)
        frames.append(blend)
    frames.extend(scene_frames)

  output_path.parent.mkdir(parents=True, exist_ok=True)
  writer = imageio.get_writer(
    output_path.as_posix(),
    fps=fps,
    codec="libx264",
    quality=8,
    macro_block_size=None,
  )
  for frame in frames:
    writer.append_data(frame)
  writer.close()


def synthesize_voiceover_say(narration_text, voice_path, voice_name, rate):
  txt_path = voice_path.with_suffix(".txt")
  txt_path.write_text(narration_text, encoding="utf-8")
  cmd = ["say", "-v", voice_name, "-r", str(rate), "-f", str(txt_path), "-o", str(voice_path)]
  subprocess.run(cmd, check=True)
  return txt_path


def synthesize_voiceover_openai(narration_text, voice_path, model, voice_name, instructions):
  api_key = os.getenv("OPENAI_API_KEY", "").strip()
  if not api_key:
    raise RuntimeError("OPENAI_API_KEY is not set.")

  payload = {
    "model": model,
    "voice": voice_name,
    "input": narration_text,
    "format": "mp3",
  }
  if instructions:
    payload["instructions"] = instructions

  req = urllib.request.Request(
    "https://api.openai.com/v1/audio/speech",
    data=json.dumps(payload).encode("utf-8"),
    headers={
      "Authorization": f"Bearer {api_key}",
      "Content-Type": "application/json",
    },
    method="POST",
  )
  with urllib.request.urlopen(req, timeout=180) as resp:
    audio = resp.read()
  voice_path.write_bytes(audio)


def mux_audio_video(video_path, audio_path, final_path):
  ffmpeg = imageio_ffmpeg.get_ffmpeg_exe()
  cmd = [
    ffmpeg,
    "-y",
    "-i",
    str(video_path),
    "-i",
    str(audio_path),
    "-c:v",
    "copy",
    "-c:a",
    "aac",
    "-b:a",
    "192k",
    "-shortest",
    str(final_path),
  ]
  subprocess.run(cmd, check=True)


def main():
  parser = argparse.ArgumentParser(description="Create marketing training video with voiceover")
  parser.add_argument("--captures-dir", required=True)
  parser.add_argument("--output-dir", required=True)
  parser.add_argument("--fps", type=int, default=12)
  parser.add_argument("--width", type=int, default=1600)
  parser.add_argument("--height", type=int, default=900)
  parser.add_argument("--voice", default="alloy")
  parser.add_argument("--rate", type=int, default=176)
  parser.add_argument("--tts-engine", choices=["openai", "say"], default="openai")
  parser.add_argument("--openai-model", default="gpt-4o-mini-tts")
  parser.add_argument("--openai-instructions", default="Premium SaaS brand voice. Confident, modern, and concise. Natural pacing, no robotic cadence.")
  parser.add_argument("--skip-tts", action="store_true")
  args = parser.parse_args()

  captures_dir = Path(args.captures_dir)
  output_dir = Path(args.output_dir)
  output_dir.mkdir(parents=True, exist_ok=True)

  scenes = build_scenes(captures_dir)
  narration = " ".join(scene["narration"] for scene in scenes)

  silent_video = output_dir / "marketing-training-silent.mp4"
  voiceover = output_dir / ("voiceover.mp3" if args.tts_engine == "openai" else "voiceover.aiff")
  final_video = output_dir / "marketing-training-video.mp4"
  narration_txt = output_dir / "narration-script.txt"
  narration_txt.write_text("\n\n".join(scene["narration"] for scene in scenes), encoding="utf-8")

  render_video(scenes, args.fps, args.width, args.height, silent_video)
  if args.skip_tts:
    final_video.write_bytes(silent_video.read_bytes())
  else:
    if args.tts_engine == "openai":
      synthesize_voiceover_openai(
        narration,
        voiceover,
        args.openai_model,
        args.voice,
        args.openai_instructions,
      )
    else:
      synthesize_voiceover_say(narration, voiceover, args.voice, args.rate)
    mux_audio_video(silent_video, voiceover, final_video)

  print(f"Created: {final_video}")
  print(f"Narration script: {narration_txt}")
  print(f"Silent video: {silent_video}")
  print(f"Voiceover audio: {voiceover}")


if __name__ == "__main__":
  main()
