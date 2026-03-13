import { getScoreColor } from "../../lib/utils";

interface ScoreRingProps {
  score: number;
  size?: number;
}

export function ScoreRing({ score, size = 44 }: ScoreRingProps) {
  const radius = (size - 6) / 2;
  const circumference = 2 * Math.PI * radius;
  const fill = (score / 100) * circumference;
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
