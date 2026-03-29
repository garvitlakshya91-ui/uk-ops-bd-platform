'use client';

import React from 'react';
import { cn } from '@/lib/utils';

interface ProgressBarProps {
  value: number;
  max?: number;
  color?: string;
  size?: 'sm' | 'md' | 'lg';
  showLabel?: boolean;
  className?: string;
  shimmer?: boolean;
  gradient?: boolean;
  label?: string;
}

export default function ProgressBar({
  value,
  max = 100,
  color,
  size = 'md',
  showLabel = false,
  className,
  shimmer = false,
  gradient = false,
  label,
}: ProgressBarProps) {
  const pct = Math.min(100, Math.max(0, (value / max) * 100));

  const autoColor = pct >= 80 ? 'bg-green-500' : pct >= 50 ? 'bg-amber-500' : 'bg-red-500';
  const barColor = gradient ? '' : (color || autoColor);

  const heights = {
    sm: 'h-1.5',
    md: 'h-2.5',
    lg: 'h-5',
  };

  return (
    <div className={cn('flex items-center gap-2', className)}>
      <div className={cn('flex-1 bg-slate-700 rounded-full overflow-hidden relative', heights[size])}>
        <div
          className={cn(
            'h-full rounded-full transition-all duration-500 relative',
            gradient ? '' : barColor
          )}
          style={{
            width: `${pct}%`,
            ...(gradient
              ? {
                  background: `linear-gradient(90deg, #ef4444 0%, #f59e0b 40%, #22c55e 100%)`,
                }
              : {}),
          }}
        >
          {shimmer && (
            <div
              className="absolute inset-0 overflow-hidden rounded-full"
              style={{
                background:
                  'linear-gradient(90deg, transparent 0%, rgba(255,255,255,0.15) 50%, transparent 100%)',
                animation: 'shimmer 2s infinite',
              }}
            />
          )}
          {label && size === 'lg' && (
            <span className="absolute inset-0 flex items-center justify-center text-xs font-bold text-white drop-shadow-sm">
              {label}
            </span>
          )}
        </div>
      </div>
      {showLabel && (
        <span className="text-xs font-medium text-slate-400 min-w-[36px] text-right">
          {Math.round(pct)}%
        </span>
      )}
      <style jsx>{`
        @keyframes shimmer {
          0% { transform: translateX(-100%); }
          100% { transform: translateX(200%); }
        }
      `}</style>
    </div>
  );
}
