'use client';

import React from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';
import { SchemeFilters, DEFAULT_FILTERS } from './FilterPanel';

interface Pill {
  key: keyof SchemeFilters;
  label: string;
  onRemove: () => void;
}

interface ActiveFilterPillsProps {
  filters: SchemeFilters;
  setFilters: (partial: Partial<SchemeFilters>) => void;
  operatorLabels: Record<number, string>;
}

export default function ActiveFilterPills({
  filters,
  setFilters,
  operatorLabels,
}: ActiveFilterPillsProps) {
  const pills: Pill[] = [];

  if (filters.source) {
    pills.push({
      key: 'source',
      label: `Source: ${filters.source}`,
      onRemove: () => setFilters({ source: DEFAULT_FILTERS.source }),
    });
  }
  if (filters.region) {
    pills.push({
      key: 'region',
      label: `Region: ${filters.region}`,
      onRemove: () => setFilters({ region: DEFAULT_FILTERS.region }),
    });
  }
  if (filters.has_owner !== 'any') {
    pills.push({
      key: 'has_owner',
      label: `Owner: ${filters.has_owner}`,
      onRemove: () => setFilters({ has_owner: 'any' }),
    });
  }
  if (filters.has_operator !== 'any') {
    pills.push({
      key: 'has_operator',
      label: `Operator: ${filters.has_operator}`,
      onRemove: () => setFilters({ has_operator: 'any' }),
    });
  }
  if (filters.has_rent !== 'any') {
    pills.push({
      key: 'has_rent',
      label: `Rent data: ${filters.has_rent}`,
      onRemove: () => setFilters({ has_rent: 'any' }),
    });
  }
  if (filters.min_units || filters.max_units) {
    const parts: string[] = [];
    if (filters.min_units) parts.push(`≥${filters.min_units}`);
    if (filters.max_units) parts.push(`≤${filters.max_units}`);
    pills.push({
      key: 'min_units',
      label: `Units ${parts.join(' ')}`,
      onRemove: () => setFilters({ min_units: '', max_units: '' }),
    });
  }
  if (filters.min_rent || filters.max_rent) {
    const parts: string[] = [];
    if (filters.min_rent) parts.push(`£${filters.min_rent}+`);
    if (filters.max_rent) parts.push(`≤£${filters.max_rent}`);
    pills.push({
      key: 'min_rent',
      label: `Rent/wk ${parts.join(' ')}`,
      onRemove: () => setFilters({ min_rent: '', max_rent: '' }),
    });
  }
  if (filters.contract_end_within_days) {
    pills.push({
      key: 'contract_end_within_days',
      label: `Expires ≤${filters.contract_end_within_days}d`,
      onRemove: () => setFilters({ contract_end_within_days: '' }),
    });
  }
  if (filters.operator_ids.length > 0) {
    const names = filters.operator_ids
      .map(id => operatorLabels[id] || `#${id}`)
      .slice(0, 3)
      .join(', ');
    const more = filters.operator_ids.length > 3 ? ` +${filters.operator_ids.length - 3}` : '';
    pills.push({
      key: 'operator_ids',
      label: `Operator: ${names}${more}`,
      onRemove: () => setFilters({ operator_ids: [] }),
    });
  }

  if (pills.length === 0) return null;

  return (
    <div className="flex flex-wrap items-center gap-1.5">
      {pills.map(p => (
        <span
          key={p.key}
          className="inline-flex items-center gap-1 px-2 py-0.5 text-[11px] font-medium bg-blue-500/15 text-blue-300 border border-blue-500/30 rounded-full"
        >
          {p.label}
          <button onClick={p.onRemove} className="hover:text-blue-200">
            <XMarkIcon className="w-3 h-3" />
          </button>
        </span>
      ))}
    </div>
  );
}
