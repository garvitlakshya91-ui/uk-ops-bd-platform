'use client';

import React, { useState } from 'react';
import { PencilSquareIcon, CheckIcon, XMarkIcon, LockClosedIcon } from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';
import { patchSchemeField } from '@/lib/api';
import { cn } from '@/lib/utils';

type FieldType = 'text' | 'number' | 'date' | 'company' | 'enum';

interface InlineFieldEditProps {
  schemeId: string | number;
  field: string;
  value: string | number | null | undefined;
  type?: FieldType;
  enumOptions?: string[];
  lockedBy?: string | null;
  canEdit?: boolean;
  displayFormatter?: (v: string | number | null | undefined) => React.ReactNode;
  onSaved?: (newValue: string | number | null) => void;
}

const TRUSTED_SOURCES = new Set([
  'manual',
  'hmlr_ccod',
  'companies_house',
  'arl_btr',
  'arl_btr_open_operating',
  'epc_new_dwelling',
  'epc',
  'operator_scraper',
  'pbsa_operator',
]);

function sourceLabel(src: string): string {
  const map: Record<string, string> = {
    manual: 'Manual',
    hmlr_ccod: 'HMLR',
    companies_house: 'Companies House',
    arl_btr: 'ARL BTR',
    arl_btr_open_operating: 'ARL BTR',
    epc_new_dwelling: 'EPC',
    epc: 'EPC',
    operator_scraper: 'Operator',
    pbsa_operator: 'Operator',
    find_a_tender: 'FTS',
    contracts_finder: 'Contracts',
    ai_enrichment: 'AI',
    ai_enrichment_batch: 'AI',
    unknown: 'Unknown',
  };
  return map[src] || src;
}

export default function InlineFieldEdit({
  schemeId,
  field,
  value,
  type = 'text',
  enumOptions,
  lockedBy,
  canEdit = true,
  displayFormatter,
  onSaved,
}: InlineFieldEditProps) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState<string>(value == null ? '' : String(value));
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const isLockedManual = lockedBy === 'manual';
  const isTrustedSource = lockedBy && TRUSTED_SOURCES.has(lockedBy);

  const display = displayFormatter
    ? displayFormatter(value)
    : value == null || value === ''
    ? <span className="text-slate-600">--</span>
    : String(value);

  async function handleSave() {
    setSaving(true);
    setError(null);
    try {
      let parsed: unknown = draft.trim();
      if (parsed === '') parsed = null;
      else if (type === 'number') {
        const n = Number(parsed);
        if (Number.isNaN(n)) {
          setError('Must be a number');
          setSaving(false);
          return;
        }
        parsed = n;
      }
      const res = await patchSchemeField(schemeId, field, parsed);
      toast.success(res.message || 'Saved');
      setEditing(false);
      onSaved?.(parsed as string | number | null);
    } catch (err: any) {
      const msg = err?.response?.data?.detail || err?.message || 'Save failed';
      setError(String(msg));
    } finally {
      setSaving(false);
    }
  }

  function handleCancel() {
    setDraft(value == null ? '' : String(value));
    setEditing(false);
    setError(null);
  }

  if (editing) {
    return (
      <div className="flex flex-col gap-1 w-full">
        <div className="flex items-center gap-1.5">
          {type === 'enum' && enumOptions ? (
            <select
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              className="flex-1 bg-slate-700 border border-slate-600 rounded px-2 py-1 text-xs text-white focus:outline-none focus:ring-1 focus:ring-blue-500/40"
              autoFocus
            >
              <option value="">--</option>
              {enumOptions.map((opt) => (
                <option key={opt} value={opt}>{opt}</option>
              ))}
            </select>
          ) : (
            <input
              type={type === 'number' ? 'number' : type === 'date' ? 'date' : 'text'}
              value={draft}
              onChange={(e) => setDraft(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') handleSave();
                if (e.key === 'Escape') handleCancel();
              }}
              className="flex-1 bg-slate-700 border border-slate-600 rounded px-2 py-1 text-xs text-white focus:outline-none focus:ring-1 focus:ring-blue-500/40"
              autoFocus
            />
          )}
          <button
            onClick={handleSave}
            disabled={saving}
            className="p-1 text-emerald-400 hover:bg-emerald-500/20 rounded disabled:opacity-50"
            title="Save"
          >
            <CheckIcon className="w-3.5 h-3.5" />
          </button>
          <button
            onClick={handleCancel}
            disabled={saving}
            className="p-1 text-slate-400 hover:bg-slate-700 rounded"
            title="Cancel"
          >
            <XMarkIcon className="w-3.5 h-3.5" />
          </button>
        </div>
        {error && <span className="text-[10px] text-red-400">{error}</span>}
      </div>
    );
  }

  return (
    <div className="inline-flex items-center gap-1.5 group">
      <span className={cn(
        'text-slate-300 text-xs',
        isLockedManual && 'text-emerald-300'
      )}>
        {display}
      </span>
      {isLockedManual && (
        <span
          className="inline-flex items-center gap-0.5 px-1.5 py-0.5 text-[9px] font-medium text-emerald-400 bg-emerald-500/10 border border-emerald-500/30 rounded-full"
          title="Manually set — locked from automated overwrites"
        >
          <LockClosedIcon className="w-2.5 h-2.5" />
          Manual
        </span>
      )}
      {!isLockedManual && isTrustedSource && lockedBy && (
        <span
          className="px-1.5 py-0.5 text-[9px] font-medium text-slate-400 bg-slate-700/50 border border-slate-600/50 rounded-full"
          title={`Source: ${lockedBy}`}
        >
          {sourceLabel(lockedBy)}
        </span>
      )}
      {canEdit && (
        <button
          onClick={() => setEditing(true)}
          className="opacity-0 group-hover:opacity-100 p-0.5 text-slate-500 hover:text-blue-400 transition-opacity"
          title="Edit (locks to manual)"
        >
          <PencilSquareIcon className="w-3.5 h-3.5" />
        </button>
      )}
    </div>
  );
}
