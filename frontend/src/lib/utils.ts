import { clsx, type ClassValue } from 'clsx';
import { twMerge } from 'tailwind-merge';
import { format, parseISO, formatDistanceToNow } from 'date-fns';

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

export function formatDate(date: string | Date, formatStr: string = 'dd MMM yyyy'): string {
  if (!date) return '';
  try {
    const d = typeof date === 'string' ? parseISO(date) : date;
    if (isNaN(d.getTime())) return '';
    return format(d, formatStr);
  } catch {
    return '';
  }
}

export function formatRelativeDate(date: string | Date): string {
  if (!date) return '';
  try {
    const d = typeof date === 'string' ? parseISO(date) : date;
    if (isNaN(d.getTime())) return '';
    return formatDistanceToNow(d, { addSuffix: true });
  } catch {
    return '';
  }
}

export function formatNumber(num: number): string {
  const n = num ?? 0;
  if (n >= 1000000000) {
    return `${(n / 1000000000).toFixed(1)}B`;
  }
  if (n >= 1000000) {
    return `${(n / 1000000).toFixed(1)}M`;
  }
  if (n >= 1000) {
    return `${(n / 1000).toFixed(1)}k`;
  }
  return n.toLocaleString();
}

export function formatCurrency(num: number): string {
  return new Intl.NumberFormat('en-GB', {
    style: 'currency',
    currency: 'GBP',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(num ?? 0);
}

export function getStatusColor(status: string): string {
  const colors: Record<string, string> = {
    submitted: 'bg-yellow-500/20 text-yellow-400 border-yellow-500/30',
    validated: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
    approved: 'bg-green-500/20 text-green-400 border-green-500/30',
    refused: 'bg-red-500/20 text-red-400 border-red-500/30',
    withdrawn: 'bg-gray-500/20 text-gray-400 border-gray-500/30',
    pending: 'bg-orange-500/20 text-orange-400 border-orange-500/30',
  };
  return colors[(status || '').toLowerCase()] || 'bg-gray-500/20 text-gray-400 border-gray-500/30';
}

export function getSchemeTypeColor(type: string): string {
  const colors: Record<string, string> = {
    btr: 'bg-purple-500/20 text-purple-400 border-purple-500/30',
    pbsa: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
    'co-living': 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30',
    'co_living': 'bg-cyan-500/20 text-cyan-400 border-cyan-500/30',
    senior: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
    'senior_living': 'bg-amber-500/20 text-amber-400 border-amber-500/30',
    affordable: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
  };
  return colors[(type || '').toLowerCase()] || 'bg-gray-500/20 text-gray-400 border-gray-500/30';
}

export function getPriorityColor(priority: string): string {
  const colors: Record<string, string> = {
    high: 'bg-red-500/20 text-red-400 border-red-500/30',
    medium: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
    low: 'bg-green-500/20 text-green-400 border-green-500/30',
  };
  return colors[(priority || '').toLowerCase()] || 'bg-gray-500/20 text-gray-400 border-gray-500/30';
}

export function getStageColor(stage: string): string {
  const colors: Record<string, string> = {
    identified: 'bg-slate-500/20 text-slate-300 border-slate-500/30',
    researched: 'bg-blue-500/20 text-blue-400 border-blue-500/30',
    contacted: 'bg-indigo-500/20 text-indigo-400 border-indigo-500/30',
    meeting: 'bg-violet-500/20 text-violet-400 border-violet-500/30',
    proposal: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
    won: 'bg-green-500/20 text-green-400 border-green-500/30',
    lost: 'bg-red-500/20 text-red-400 border-red-500/30',
  };
  return colors[(stage || '').toLowerCase()] || 'bg-gray-500/20 text-gray-400 border-gray-500/30';
}

export function getHealthColor(status: string): string {
  const colors: Record<string, string> = {
    healthy: 'bg-green-500/20 text-green-400 border-green-500/30',
    warning: 'bg-amber-500/20 text-amber-400 border-amber-500/30',
    critical: 'bg-red-500/20 text-red-400 border-red-500/30',
  };
  return colors[(status || '').toLowerCase()] || 'bg-gray-500/20 text-gray-400 border-gray-500/30';
}

export function getBdScoreColor(score: number): string {
  const s = score ?? 0;
  if (s > 80) return 'text-red-400';
  if (s > 50) return 'text-amber-400';
  return 'text-green-400';
}

export function getBdScoreBarColor(score: number): string {
  const s = score ?? 0;
  if (s > 80) return 'bg-red-500';
  if (s > 50) return 'bg-amber-500';
  return 'bg-green-500';
}

export function getContractEndColor(dateStr: string): string {
  if (!dateStr) return 'text-slate-300';
  try {
    const now = new Date();
    const end = parseISO(dateStr);
    if (isNaN(end.getTime())) return 'text-slate-300';
    const monthsDiff = (end.getFullYear() - now.getFullYear()) * 12 + (end.getMonth() - now.getMonth());
    if (monthsDiff < 6) return 'text-red-400';
    if (monthsDiff < 12) return 'text-amber-400';
    return 'text-slate-300';
  } catch {
    return 'text-slate-300';
  }
}

export const PIPELINE_STAGES = [
  'identified',
  'researched',
  'contacted',
  'meeting',
  'proposal',
  'won',
  'lost',
] as const;

export const SCHEME_TYPES = ['BTR', 'PBSA', 'Co-living', 'Senior', 'Affordable'] as const;
