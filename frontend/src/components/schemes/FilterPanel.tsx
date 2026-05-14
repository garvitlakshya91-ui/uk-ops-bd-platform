'use client';

import React, { useEffect, useRef, useState } from 'react';
import { XMarkIcon } from '@heroicons/react/24/outline';
import { cn } from '@/lib/utils';
import {
  searchOperators,
  SchemesFilterOptions,
  OperatorAutocompleteItem,
} from '@/lib/api';

export interface SchemeFilters {
  search: string;
  scheme_type: string;
  source: string;
  region: string;
  council_id: string;
  has_owner: 'any' | 'yes' | 'no';
  has_operator: 'any' | 'yes' | 'no';
  has_rent: 'any' | 'yes' | 'no';
  min_units: string;
  max_units: string;
  min_rent: string;
  max_rent: string;
  operator_ids: number[];
  contract_end_within_days: string;
}

export const DEFAULT_FILTERS: SchemeFilters = {
  search: '',
  scheme_type: '',
  source: '',
  region: '',
  council_id: '',
  has_owner: 'any',
  has_operator: 'any',
  has_rent: 'any',
  min_units: '',
  max_units: '',
  min_rent: '',
  max_rent: '',
  operator_ids: [],
  contract_end_within_days: '',
};

export function countActiveFilters(f: SchemeFilters): number {
  let n = 0;
  if (f.search) n++;
  if (f.scheme_type) n++;
  if (f.source) n++;
  if (f.region) n++;
  if (f.council_id) n++;
  if (f.has_owner !== 'any') n++;
  if (f.has_operator !== 'any') n++;
  if (f.has_rent !== 'any') n++;
  if (f.min_units) n++;
  if (f.max_units) n++;
  if (f.min_rent) n++;
  if (f.max_rent) n++;
  if (f.operator_ids.length) n++;
  if (f.contract_end_within_days) n++;
  return n;
}

interface FilterPanelProps {
  filters: SchemeFilters;
  setFilters: (partial: Partial<SchemeFilters>) => void;
  filterOptions: SchemesFilterOptions | null;
  operatorLabels: Record<number, string>;
  setOperatorLabels: (m: Record<number, string>) => void;
}

const CONTRACT_WINDOWS: Array<{ value: string; label: string }> = [
  { value: '', label: 'Any time' },
  { value: '30', label: 'Next 30 days' },
  { value: '60', label: 'Next 60 days' },
  { value: '90', label: 'Next 90 days' },
  { value: '180', label: 'Next 6 months' },
  { value: '365', label: 'Next 12 months' },
];

function TriStateRadio({
  label,
  value,
  onChange,
}: {
  label: string;
  value: 'any' | 'yes' | 'no';
  onChange: (v: 'any' | 'yes' | 'no') => void;
}) {
  const opts: Array<{ key: 'any' | 'yes' | 'no'; label: string }> = [
    { key: 'any', label: 'Any' },
    { key: 'yes', label: 'Yes' },
    { key: 'no', label: 'No' },
  ];
  return (
    <div>
      <div className="text-[11px] text-slate-400 mb-1.5">{label}</div>
      <div className="inline-flex rounded-lg bg-slate-800 border border-slate-700 p-0.5">
        {opts.map(o => (
          <button
            key={o.key}
            onClick={() => onChange(o.key)}
            className={cn(
              'px-3 py-1 text-xs font-medium rounded-md transition-colors',
              value === o.key
                ? 'bg-slate-700 text-white'
                : 'text-slate-400 hover:text-white'
            )}
          >
            {o.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function OperatorAutocomplete({
  selectedIds,
  labels,
  onChange,
  onLabelsChange,
}: {
  selectedIds: number[];
  labels: Record<number, string>;
  onChange: (ids: number[]) => void;
  onLabelsChange: (m: Record<number, string>) => void;
}) {
  const [q, setQ] = useState('');
  const [results, setResults] = useState<OperatorAutocompleteItem[]>([]);
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(async () => {
      setLoading(true);
      try {
        const items = await searchOperators(q, 10);
        setResults(items);
      } catch {
        setResults([]);
      } finally {
        setLoading(false);
      }
    }, 200);
    return () => { if (debounceRef.current) clearTimeout(debounceRef.current); };
  }, [q]);

  const add = (item: OperatorAutocompleteItem) => {
    if (!selectedIds.includes(item.id)) {
      onChange([...selectedIds, item.id]);
      onLabelsChange({ ...labels, [item.id]: item.name });
    }
    setQ('');
  };
  const remove = (id: number) => {
    onChange(selectedIds.filter(x => x !== id));
  };

  return (
    <div className="relative">
      <div className="text-[11px] text-slate-400 mb-1.5">Operator</div>
      <div className="flex flex-wrap gap-1.5 mb-2">
        {selectedIds.map(id => (
          <span
            key={id}
            className="inline-flex items-center gap-1 px-2 py-0.5 text-xs bg-blue-500/15 text-blue-300 border border-blue-500/30 rounded-full"
          >
            {labels[id] || `#${id}`}
            <button onClick={() => remove(id)} className="hover:text-blue-200">
              <XMarkIcon className="w-3 h-3" />
            </button>
          </span>
        ))}
      </div>
      <input
        value={q}
        onChange={e => { setQ(e.target.value); setOpen(true); }}
        onFocus={() => setOpen(true)}
        onBlur={() => setTimeout(() => setOpen(false), 200)}
        placeholder="Type to search operators…"
        className="w-full bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-blue-500/40"
      />
      {open && results.length > 0 && (
        <div className="absolute z-10 left-0 right-0 mt-1 max-h-64 overflow-y-auto bg-slate-800 border border-slate-700 rounded-lg shadow-xl">
          {results.map(r => {
            const already = selectedIds.includes(r.id);
            return (
              <button
                key={r.id}
                onMouseDown={(e) => { e.preventDefault(); add(r); }}
                disabled={already}
                className={cn(
                  'w-full flex items-center justify-between px-3 py-2 text-xs border-b border-slate-700/50 last:border-b-0',
                  already ? 'opacity-40 cursor-not-allowed' : 'hover:bg-slate-700/50 cursor-pointer'
                )}
              >
                <span className="text-slate-200 truncate">{r.name}</span>
                <span className="flex-shrink-0 text-[10px] text-slate-500 ml-2">{r.scheme_count} schemes</span>
              </button>
            );
          })}
        </div>
      )}
      {open && !loading && q && results.length === 0 && (
        <div className="absolute z-10 left-0 right-0 mt-1 px-3 py-2 text-xs text-slate-500 bg-slate-800 border border-slate-700 rounded-lg">
          No operators match &quot;{q}&quot;
        </div>
      )}
    </div>
  );
}

export default function FilterPanel({
  filters,
  setFilters,
  filterOptions,
  operatorLabels,
  setOperatorLabels,
}: FilterPanelProps) {
  return (
    <div className="border border-slate-700 bg-slate-800/40 rounded-lg p-4 space-y-5">
      {/* Data quality */}
      <section>
        <h4 className="text-xs font-semibold text-slate-300 uppercase tracking-wide mb-2">Data quality</h4>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          <TriStateRadio
            label="Has owner"
            value={filters.has_owner}
            onChange={v => setFilters({ has_owner: v })}
          />
          <TriStateRadio
            label="Has operator"
            value={filters.has_operator}
            onChange={v => setFilters({ has_operator: v })}
          />
          <TriStateRadio
            label="Has rent data"
            value={filters.has_rent}
            onChange={v => setFilters({ has_rent: v })}
          />
        </div>
      </section>

      {/* Size + Location + Source */}
      <section>
        <h4 className="text-xs font-semibold text-slate-300 uppercase tracking-wide mb-2">Attributes</h4>
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3">
          <div>
            <div className="text-[11px] text-slate-400 mb-1.5">Min units</div>
            <input
              type="number"
              min={0}
              value={filters.min_units}
              onChange={e => setFilters({ min_units: e.target.value })}
              placeholder="0"
              className="w-full bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-blue-500/40"
            />
          </div>
          <div>
            <div className="text-[11px] text-slate-400 mb-1.5">Max units</div>
            <input
              type="number"
              min={0}
              value={filters.max_units}
              onChange={e => setFilters({ max_units: e.target.value })}
              placeholder="∞"
              className="w-full bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-blue-500/40"
            />
          </div>
          <div>
            <div className="text-[11px] text-slate-400 mb-1.5">Region</div>
            <select
              value={filters.region}
              onChange={e => setFilters({ region: e.target.value })}
              className="w-full bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-xs text-white focus:outline-none focus:ring-1 focus:ring-blue-500/40"
            >
              <option value="">All regions</option>
              {(filterOptions?.regions || []).map(r => (
                <option key={r} value={r}>{r}</option>
              ))}
            </select>
          </div>
          <div>
            <div className="text-[11px] text-slate-400 mb-1.5">Source</div>
            <select
              value={filters.source}
              onChange={e => setFilters({ source: e.target.value })}
              className="w-full bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-xs text-white focus:outline-none focus:ring-1 focus:ring-blue-500/40"
            >
              <option value="">All sources</option>
              {(filterOptions?.sources || []).map(s => (
                <option key={s.value} value={s.value}>
                  {s.label || s.value} ({s.count.toLocaleString()})
                </option>
              ))}
            </select>
          </div>
        </div>
      </section>

      {/* Rent + Operator + Contract timing */}
      <section>
        <h4 className="text-xs font-semibold text-slate-300 uppercase tracking-wide mb-2">Commercial</h4>
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <div className="text-[11px] text-slate-400 mb-1.5">Min rent £/wk</div>
              <input
                type="number"
                min={0}
                value={filters.min_rent}
                onChange={e => setFilters({ min_rent: e.target.value })}
                placeholder="0"
                className="w-full bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-blue-500/40"
              />
            </div>
            <div>
              <div className="text-[11px] text-slate-400 mb-1.5">Max rent £/wk</div>
              <input
                type="number"
                min={0}
                value={filters.max_rent}
                onChange={e => setFilters({ max_rent: e.target.value })}
                placeholder="∞"
                className="w-full bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-xs text-white placeholder-slate-500 focus:outline-none focus:ring-1 focus:ring-blue-500/40"
              />
            </div>
          </div>
          <div>
            <div className="text-[11px] text-slate-400 mb-1.5">Contract expires</div>
            <select
              value={filters.contract_end_within_days}
              onChange={e => setFilters({ contract_end_within_days: e.target.value })}
              className="w-full bg-slate-800 border border-slate-700 rounded px-2 py-1.5 text-xs text-white focus:outline-none focus:ring-1 focus:ring-blue-500/40"
            >
              {CONTRACT_WINDOWS.map(w => (
                <option key={w.value} value={w.value}>{w.label}</option>
              ))}
            </select>
          </div>
        </div>
        <div className="mt-3">
          <OperatorAutocomplete
            selectedIds={filters.operator_ids}
            labels={operatorLabels}
            onChange={ids => setFilters({ operator_ids: ids })}
            onLabelsChange={setOperatorLabels}
          />
        </div>
      </section>
    </div>
  );
}
