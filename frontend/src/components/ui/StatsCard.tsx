'use client';

import React, { useEffect, useState, useRef } from 'react';
import { cn } from '@/lib/utils';
import { ArrowTrendingUpIcon, ArrowTrendingDownIcon } from '@heroicons/react/24/solid';

interface StatsCardProps {
  label: string;
  value: string | number;
  trend?: number;
  icon?: React.ReactNode;
  className?: string;
  accentColor?: 'blue' | 'purple' | 'cyan' | 'amber' | 'red' | 'green';
  sparkline?: number[];
  circularProgress?: number;
}

const accentGradients: Record<string, string> = {
  blue: 'linear-gradient(180deg, #3b82f6, #2563eb)',
  purple: 'linear-gradient(180deg, #a855f7, #7c3aed)',
  cyan: 'linear-gradient(180deg, #06b6d4, #0891b2)',
  amber: 'linear-gradient(180deg, #f59e0b, #d97706)',
  red: 'linear-gradient(180deg, #ef4444, #dc2626)',
  green: 'linear-gradient(180deg, #22c55e, #16a34a)',
};

const accentIconBg: Record<string, string> = {
  blue: 'bg-blue-500/10 text-blue-400',
  purple: 'bg-purple-500/10 text-purple-400',
  cyan: 'bg-cyan-500/10 text-cyan-400',
  amber: 'bg-amber-500/10 text-amber-400',
  red: 'bg-red-500/10 text-red-400',
  green: 'bg-green-500/10 text-green-400',
};

function MiniSparkline({ data, color = '#3b82f6' }: { data: number[]; color?: string }) {
  if (!data || data.length < 2) return null;
  const max = Math.max(...data);
  const min = Math.min(...data);
  const range = max - min || 1;
  const w = 80;
  const h = 28;
  const points = data.map((v, i) => {
    const x = (i / (data.length - 1)) * w;
    const y = h - ((v - min) / range) * (h - 4) - 2;
    return `${x},${y}`;
  });
  const pathD = `M${points.join(' L')}`;
  const areaD = `${pathD} L${w},${h} L0,${h} Z`;

  return (
    <svg width={w} height={h} className="overflow-visible">
      <defs>
        <linearGradient id={`spark-${color.replace('#', '')}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stopColor={color} stopOpacity="0.3" />
          <stop offset="100%" stopColor={color} stopOpacity="0" />
        </linearGradient>
      </defs>
      <path d={areaD} fill={`url(#spark-${color.replace('#', '')})`} />
      <path d={pathD} fill="none" stroke={color} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function CircularProgress({ value, size = 44, strokeWidth = 3.5, color = '#3b82f6' }: { value: number; size?: number; strokeWidth?: number; color?: string }) {
  const radius = (size - strokeWidth) / 2;
  const circumference = 2 * Math.PI * radius;
  const offset = circumference - (value / 100) * circumference;

  return (
    <svg width={size} height={size} className="-rotate-90">
      <circle cx={size / 2} cy={size / 2} r={radius} fill="none" stroke="#334155" strokeWidth={strokeWidth} />
      <circle
        cx={size / 2}
        cy={size / 2}
        r={radius}
        fill="none"
        stroke={color}
        strokeWidth={strokeWidth}
        strokeDasharray={circumference}
        strokeDashoffset={offset}
        strokeLinecap="round"
        className="transition-all duration-1000 ease-out"
      />
    </svg>
  );
}

function useCountUp(end: number, duration: number = 800) {
  const [count, setCount] = useState(0);
  const frameRef = useRef<number>();

  useEffect(() => {
    let start = 0;
    const startTime = performance.now();

    const animate = (currentTime: number) => {
      const elapsed = currentTime - startTime;
      const progress = Math.min(elapsed / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      setCount(Math.floor(eased * end));

      if (progress < 1) {
        frameRef.current = requestAnimationFrame(animate);
      }
    };

    frameRef.current = requestAnimationFrame(animate);
    return () => {
      if (frameRef.current) cancelAnimationFrame(frameRef.current);
    };
  }, [end, duration]);

  return count;
}

export default function StatsCard({
  label,
  value,
  trend,
  icon,
  className,
  accentColor = 'blue',
  sparkline,
  circularProgress,
}: StatsCardProps) {
  const numericValue = typeof value === 'string' ? parseFloat(value.replace(/[^0-9.]/g, '')) : value;
  const animatedValue = useCountUp(isNaN(numericValue) ? 0 : numericValue);
  const displayValue = typeof value === 'string'
    ? value.replace(/[\d,.]+/, animatedValue.toLocaleString())
    : animatedValue.toLocaleString();

  const sparklineColors: Record<string, string> = {
    blue: '#3b82f6',
    purple: '#a855f7',
    cyan: '#06b6d4',
    amber: '#f59e0b',
    red: '#ef4444',
    green: '#22c55e',
  };

  return (
    <div
      className={cn(
        'relative bg-slate-800/90 border border-slate-700/50 rounded-xl p-5 shadow-lg card-hover-lift overflow-hidden group',
        className
      )}
    >
      {/* Gradient left accent */}
      <div
        className="absolute left-0 top-3 bottom-3 w-[3px] rounded-r-full"
        style={{ background: accentGradients[accentColor] }}
      />

      {/* Subtle hover gradient overlay */}
      <div className="absolute inset-0 opacity-0 group-hover:opacity-100 transition-opacity duration-300 bg-gradient-to-br from-blue-500/[0.02] to-purple-500/[0.02] pointer-events-none" />

      <div className="flex items-start justify-between relative">
        <div className="min-w-0 flex-1">
          <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">{label}</p>
          <p className="mt-2 text-2xl font-bold text-white animate-count-up">{displayValue}</p>
          {trend !== undefined && (
            <div className="mt-1.5 flex items-center gap-1">
              {trend >= 0 ? (
                <ArrowTrendingUpIcon className="w-3.5 h-3.5 text-emerald-400" />
              ) : (
                <ArrowTrendingDownIcon className="w-3.5 h-3.5 text-red-400" />
              )}
              <span
                className={cn(
                  'text-xs font-semibold',
                  trend >= 0 ? 'text-emerald-400' : 'text-red-400'
                )}
              >
                {Math.abs(trend)}%
              </span>
              <span className="text-[10px] text-slate-500">vs last week</span>
            </div>
          )}
        </div>
        <div className="flex flex-col items-end gap-2">
          {icon && (
            <div className={cn('p-2.5 rounded-lg', accentIconBg[accentColor])}>
              {icon}
            </div>
          )}
          {sparkline && (
            <MiniSparkline data={sparkline} color={sparklineColors[accentColor]} />
          )}
          {circularProgress !== undefined && (
            <div className="relative flex items-center justify-center">
              <CircularProgress value={circularProgress} color={sparklineColors[accentColor]} />
              <span className="absolute text-[10px] font-bold text-slate-300">{circularProgress}%</span>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
