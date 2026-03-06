#!/usr/bin/env python3
import asyncio
import argparse
from pathlib import Path

import edge_tts


async def main():
  parser = argparse.ArgumentParser(description="Synthesize speech with Edge TTS")
  parser.add_argument("--text-file", required=True)
  parser.add_argument("--output", required=True)
  parser.add_argument("--voice", default="en-US-JennyNeural")
  parser.add_argument("--rate", default="+8%")
  parser.add_argument("--pitch", default="+0Hz")
  args = parser.parse_args()

  text = Path(args.text_file).read_text(encoding="utf-8").strip()
  if not text:
    raise SystemExit("Text file is empty")

  communicate = edge_tts.Communicate(text=text, voice=args.voice, rate=args.rate, pitch=args.pitch)
  await communicate.save(args.output)
  print(f"Created: {args.output}")


if __name__ == "__main__":
  asyncio.run(main())
