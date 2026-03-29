'use client';

import React from 'react';
import { cn } from '@/lib/utils';

interface BadgeProps {
  children: React.ReactNode;
  variant?: string;
  className?: string;
  size?: 'sm' | 'md' | 'lg';
  dot?: boolean;
  dotColor?: string;
  icon?: React.ReactNode;
  pulse?: boolean;
}

export default function Badge({
  children,
  variant,
  className,
  size = 'sm',
  dot,
  dotColor,
  icon,
  pulse,
}: BadgeProps) {
  const sizeClasses = {
    sm: 'px-2 py-0.5 text-xs',
    md: 'px-3 py-1 text-sm',
    lg: 'px-4 py-1.5 text-sm font-semibold',
  };

  return (
    <span
      className={cn(
        'inline-flex items-center gap-1.5 font-medium rounded-full border',
        sizeClasses[size],
        variant || 'bg-slate-500/20 text-slate-300 border-slate-500/30',
        pulse && 'animate-pulse',
        className
      )}
    >
      {dot && (
        <span
          className={cn(
            'w-1.5 h-1.5 rounded-full flex-shrink-0',
            dotColor || 'bg-current'
          )}
        />
      )}
      {icon && <span className="flex-shrink-0">{icon}</span>}
      {children}
    </span>
  );
}
