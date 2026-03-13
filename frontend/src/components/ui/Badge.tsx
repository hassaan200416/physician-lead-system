interface BadgeProps {
  children: React.ReactNode;
  className?: string;
}

export function Badge({ children, className = "" }: BadgeProps) {
  return (
    <span
      className={`
      inline-flex items-center px-2 py-0.5 rounded-md text-xs font-medium
      border font-mono tracking-wide ${className}
    `}
    >
      {children}
    </span>
  );
}
