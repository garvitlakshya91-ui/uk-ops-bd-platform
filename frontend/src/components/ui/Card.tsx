'use client';

import React from 'react';
import { cn } from '@/lib/utils';

interface CardProps {
  title?: string;
  subtitle?: string;
  action?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  noPadding?: boolean;
  variant?: 'default' | 'glass';
  gradientBorder?: 'blue' | 'purple' | 'cyan' | 'amber' | 'red' | 'green' | 'none';
  headerGradient?: boolean;
  hoverLift?: boolean;
}

const gradientBorderColors: Record<string, string> = {
  blue: 'from-blue-500 to-blue-600',
  purple: 'from-purple-500 to-violet-600',
  cyan: 'from-cyan-400 to-blue-500',
  amber: 'from-amber-400 to-orange-500',
  red: 'from-red-400 to-rose-600',
  green: 'from-emerald-400 to-green-600',
};

export default function Card({
  title,
  subtitle,
  action,
  children,
  className,
  noPadding,
  variant = 'default',
  gradientBorder = 'none',
  headerGradient = false,
  hoverLift = false,
}: CardProps) {
  const baseStyles =
    variant === 'glass'
      ? 'glass-card-premium relative'
      : 'bg-slate-800/90 border border-slate-700/50 rounded-xl shadow-lg';

  return (
    <div
      className={cn(
        baseStyles,
        'rounded-xl overflow-hidden',
        hoverLift && 'card-hover-lift',
        className
      )}
    >
      {gradientBorder !== 'none' && (
        <div
          className={cn(
            'absolute top-0 left-0 right-0 h-[2px] bg-gradient-to-r',
            gradientBorderColors[gradientBorder]
          )}
        />
      )}
      {(title || action) && (
        <div
          className={cn(
            'flex items-center justify-between px-6 py-4 border-b border-slate-700/50',
            headerGradient &&
              'bg-gradient-to-r from-slate-800 via-slate-800/95 to-slate-800/90'
          )}
        >
          <div>
            {title && <h3 className="text-lg font-semibold text-white">{title}</h3>}
            {subtitle && <p className="text-sm text-slate-400 mt-0.5">{subtitle}</p>}
          </div>
          {action && <div>{action}</div>}
        </div>
      )}
      <div className={cn(!noPadding && 'p-6')}>{children}</div>
    </div>
  );
}
