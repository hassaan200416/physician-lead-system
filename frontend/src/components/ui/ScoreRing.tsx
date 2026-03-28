/**
 * ScoreRing.tsx
 * -------------
 * SVG circular progress indicator that displays a 0-100 lead score.
 * The ring is drawn with two stacked <circle> elements:
 *   - background track: full circle, low-opacity white
 *   - progress arc:     stroke-dasharray = full circumference,
 *                       stroke-dashoffset = circumference × (1 - score/100)
 * The SVG is rotated -90° so the arc starts at the top (12 o'clock).
 * Color progresses from grey → amber → teal → emerald as score increases.
 */
import { getScoreColor } from "../../lib/utils";

interface ScoreRingProps {
  score: number;
  /** Outer diameter in pixels. Default 44. Pass 56 for the drawer's larger ring. */
  size?: number;
}

export function ScoreRing({ score, size = 44 }: ScoreRingProps) {
  const radius = (size - 6) / 2;            // inner radius leaving 3px stroke room
  const circumference = 2 * Math.PI * radius;
  const fill = (score / 100) * circumference; // arc length proportional to score
  const color = getScoreColor(score);

  return (
    <div className="relative inline-flex items-center justify-center">
      <svg width={size} height={size} className="-rotate-90">
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke="rgba(255,255,255,0.06)"
          strokeWidth={3}
        />
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke={color}
          strokeWidth={3}
          strokeDasharray={circumference}
          strokeDashoffset={circumference - fill}
          strokeLinecap="round"
          style={{ transition: "stroke-dashoffset 1s ease" }}
        />
      </svg>
      <span
        className="absolute text-xs font-mono font-semibold"
        style={{ color, fontSize: size < 40 ? "10px" : "11px" }}
      >
        {Math.round(score)}
      </span>
    </div>
  );
}
