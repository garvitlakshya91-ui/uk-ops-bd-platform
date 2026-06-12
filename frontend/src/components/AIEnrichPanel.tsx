'use client';

import React, { useState } from 'react';
import {
  SparklesIcon,
  CheckCircleIcon,
  XCircleIcon,
  ArrowPathIcon,
  ChevronDownIcon,
  ChevronUpIcon,
} from '@heroicons/react/24/outline';
import { cn } from '@/lib/utils';
import { aiEnrichScheme, applyAISuggestions, AIFieldSuggestion, AIRentSuggestion } from '@/lib/api';
import toast from 'react-hot-toast';

interface AIEnrichPanelProps {
  schemeId: string;
  schemeName: string;
  onApplied?: () => void;
}

const FIELD_LABELS: Record<string, string> = {
  owner_company_name: 'Owner / Developer',
  operator_company_name: 'Operator',
  asset_manager_company_name: 'Asset Manager',
  landlord_company_name: 'Landlord',
  num_units: 'Number of Units',
  scheme_type: 'Scheme Type',
  status: 'Status',
  address: 'Address',
  postcode: 'Postcode',
};

function confidenceColor(c: number): string {
  if (c >= 0.8) return 'text-emerald-400';
  if (c >= 0.5) return 'text-amber-400';
  return 'text-red-400';
}

function confidenceBg(c: number): string {
  if (c >= 0.8) return 'bg-emerald-500/20 border-emerald-500/40';
  if (c >= 0.5) return 'bg-amber-500/20 border-amber-500/40';
  return 'bg-red-500/20 border-red-500/40';
}

function confidenceBar(c: number): string {
  if (c >= 0.8) return 'bg-emerald-500';
  if (c >= 0.5) return 'bg-amber-500';
  return 'bg-red-500';
}

export default function AIEnrichPanel({ schemeId, schemeName, onApplied }: AIEnrichPanelProps) {
  const [loading, setLoading] = useState(false);
  const [suggestions, setSuggestions] = useState<AIFieldSuggestion[] | null>(null);
  const [rents, setRents] = useState<AIRentSuggestion[]>([]);
  const [selectedRents, setSelectedRents] = useState<Set<number>>(new Set());
  const [aiNotes, setAiNotes] = useState<string | null>(null);
  const [selected, setSelected] = useState<Set<string>>(new Set());
  const [applying, setApplying] = useState(false);
  const [applied, setApplied] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleEnrich = async (e: React.MouseEvent) => {
    e.stopPropagation();
    setLoading(true);
    setError(null);
    setSuggestions(null);
    setApplied(false);
    setExpanded(true);

    try {
      const result = await aiEnrichScheme(schemeId);
      setSuggestions(result.suggestions);
      setRents(result.rents || []);
      setAiNotes(result.raw_ai_notes);
      // Auto-select high-confidence suggestions and rents
      const autoSelect = new Set<string>();
      result.suggestions.forEach((s) => {
        if (s.confidence >= 0.7 && s.suggested_value) {
          autoSelect.add(s.field);
        }
      });
      setSelected(autoSelect);
      const autoSelectRents = new Set<number>();
      (result.rents || []).forEach((r, idx) => {
        if (r.confidence >= 0.7 && (r.rent_per_week || r.rent_per_month)) {
          autoSelectRents.add(idx);
        }
      });
      setSelectedRents(autoSelectRents);
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'AI enrichment failed';
      setError(msg);
      toast.error(msg);
    } finally {
      setLoading(false);
    }
  };

  const handleApply = async (e: React.MouseEvent) => {
    e.stopPropagation();
    if (!suggestions || (selected.size === 0 && selectedRents.size === 0)) return;

    setApplying(true);
    try {
      const toApply = suggestions
        .filter((s) => selected.has(s.field))
        .map((s) => ({ field: s.field, value: s.suggested_value }));
      const toApplyRents = rents.filter((_, idx) => selectedRents.has(idx));

      const result = await applyAISuggestions(schemeId, toApply, toApplyRents);
      setApplied(true);
      const msg = `Applied ${result.applied_fields.length} field(s)` +
        (result.rents_saved ? ` and ${result.rents_saved} rent tier(s)` : '') +
        ` to ${schemeName}`;
      toast.success(msg);
      onApplied?.();
    } catch (err: unknown) {
      const msg = (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail || 'Failed to apply suggestions';
      toast.error(msg);
    } finally {
      setApplying(false);
    }
  };

  const toggleSelectRent = (idx: number) => {
    setSelectedRents((prev) => {
      const next = new Set(prev);
      if (next.has(idx)) next.delete(idx);
      else next.add(idx);
      return next;
    });
  };

  const toggleSelect = (field: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(field)) {
        next.delete(field);
      } else {
        next.add(field);
      }
      return next;
    });
  };

  const selectAll = () => {
    if (!suggestions) return;
    if (selected.size === suggestions.length) {
      setSelected(new Set());
    } else {
      setSelected(new Set(suggestions.map((s) => s.field)));
    }
  };

  // Compact state - just the button
  if (!suggestions && !loading && !error) {
    return (
      <button
        onClick={handleEnrich}
        className="inline-flex items-center gap-2 px-4 py-2.5 text-sm font-medium text-amber-300 bg-gradient-to-r from-amber-500/10 to-orange-500/10 border border-amber-500/30 rounded-lg hover:from-amber-500/20 hover:to-orange-500/20 hover:border-amber-500/50 transition-all shadow-lg shadow-amber-500/5"
      >
        <SparklesIcon className="w-5 h-5" />
        Search with AI
      </button>
    );
  }

  return (
    <div className="mt-4 rounded-lg border border-amber-500/20 bg-gradient-to-br from-amber-500/[0.04] to-orange-500/[0.04] overflow-hidden">
      {/* Header */}
      <div
        className="flex items-center justify-between px-4 py-3 cursor-pointer hover:bg-amber-500/[0.03] transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <div className="flex items-center gap-2">
          <SparklesIcon className="w-4 h-4 text-amber-400" />
          <span className="text-sm font-semibold text-white">AI Enrichment</span>
          {loading && (
            <span className="flex items-center gap-1 text-xs text-amber-400">
              <ArrowPathIcon className="w-3.5 h-3.5 animate-spin" />
              Analysing...
            </span>
          )}
          {applied && (
            <span className="flex items-center gap-1 text-xs text-emerald-400">
              <CheckCircleIcon className="w-3.5 h-3.5" />
              Applied
            </span>
          )}
          {error && (
            <span className="flex items-center gap-1 text-xs text-red-400">
              <XCircleIcon className="w-3.5 h-3.5" />
              Error
            </span>
          )}
          {suggestions && !applied && (
            <span className="text-xs text-slate-400">
              {suggestions.length} suggestion{suggestions.length !== 1 ? 's' : ''}
            </span>
          )}
        </div>
        <div className="flex items-center gap-2">
          {!loading && (
            <button
              onClick={handleEnrich}
              className="text-xs text-amber-400 hover:text-amber-300 transition-colors"
              title="Re-run AI analysis"
            >
              <ArrowPathIcon className="w-4 h-4" />
            </button>
          )}
          {expanded ? (
            <ChevronUpIcon className="w-4 h-4 text-slate-500" />
          ) : (
            <ChevronDownIcon className="w-4 h-4 text-slate-500" />
          )}
        </div>
      </div>

      {/* Expanded content */}
      {expanded && (
        <div className="px-4 pb-4 space-y-3">
          {/* Loading skeleton */}
          {loading && (
            <div className="space-y-2">
              {[1, 2, 3].map((i) => (
                <div key={i} className="h-12 bg-slate-700/30 rounded-lg animate-pulse" />
              ))}
            </div>
          )}

          {/* Error */}
          {error && (
            <div className="bg-red-500/10 border border-red-500/30 rounded-lg p-3">
              <p className="text-xs text-red-400">{error}</p>
              <button
                onClick={handleEnrich}
                className="mt-2 text-xs text-red-300 underline hover:text-red-200"
              >
                Try again
              </button>
            </div>
          )}

          {/* Suggestions list */}
          {suggestions && suggestions.length > 0 && !applied && (
            <>
              {/* Select all toggle */}
              <div className="flex items-center justify-between">
                <button
                  onClick={selectAll}
                  className="text-[11px] text-slate-400 hover:text-slate-300 transition-colors"
                >
                  {selected.size === suggestions.length ? 'Deselect all' : 'Select all'}
                </button>
                <span className="text-[11px] text-slate-500">
                  {selected.size} of {suggestions.length} selected
                </span>
              </div>

              {/* Suggestion cards */}
              <div className="space-y-2">
                {suggestions.map((s) => (
                  <div
                    key={s.field}
                    onClick={() => toggleSelect(s.field)}
                    className={cn(
                      'flex items-start gap-3 rounded-lg p-3 border cursor-pointer transition-all',
                      selected.has(s.field)
                        ? 'bg-amber-500/10 border-amber-500/30'
                        : 'bg-slate-700/20 border-slate-700/40 hover:border-slate-600'
                    )}
                  >
                    {/* Checkbox */}
                    <div className={cn(
                      'mt-0.5 flex-shrink-0 w-4 h-4 rounded border flex items-center justify-center transition-colors',
                      selected.has(s.field)
                        ? 'bg-amber-500 border-amber-500'
                        : 'border-slate-600 bg-transparent'
                    )}>
                      {selected.has(s.field) && (
                        <svg className="w-3 h-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                        </svg>
                      )}
                    </div>

                    {/* Content */}
                    <div className="flex-1 min-w-0">
                      <div className="flex items-center justify-between">
                        <span className="text-xs font-medium text-slate-300">
                          {FIELD_LABELS[s.field] || s.field}
                        </span>
                        <div className="flex items-center gap-2">
                          <div className="w-16 h-1.5 bg-slate-700 rounded-full overflow-hidden">
                            <div
                              className={cn('h-full rounded-full', confidenceBar(s.confidence))}
                              style={{ width: `${s.confidence * 100}%` }}
                            />
                          </div>
                          <span className={cn('text-[10px] font-mono', confidenceColor(s.confidence))}>
                            {Math.round(s.confidence * 100)}%
                          </span>
                        </div>
                      </div>
                      <div className="mt-1 flex items-center gap-2 text-xs">
                        {s.current_value && (
                          <>
                            <span className="text-slate-500 line-through">{s.current_value}</span>
                            <span className="text-slate-600">&rarr;</span>
                          </>
                        )}
                        <span className={cn(
                          'font-medium',
                          selected.has(s.field) ? 'text-amber-300' : 'text-slate-300'
                        )}>
                          {s.suggested_value || 'Unknown'}
                        </span>
                      </div>
                      {s.reasoning && (
                        <p className="mt-1 text-[11px] text-slate-500 leading-relaxed">{s.reasoning}</p>
                      )}
                    </div>
                  </div>
                ))}
              </div>

              {/* Rent suggestions */}
              {rents.length > 0 && (
                <div className="mt-2 space-y-2">
                  <div className="flex items-center justify-between">
                    <span className="text-[11px] font-semibold text-slate-300 uppercase tracking-wide">
                      Rent tiers ({rents.length})
                    </span>
                    <span className="text-[10px] text-slate-500">
                      {selectedRents.size} selected
                    </span>
                  </div>
                  {rents.map((r, idx) => (
                    <div
                      key={idx}
                      onClick={() => toggleSelectRent(idx)}
                      className={cn(
                        'flex items-start gap-3 rounded-lg p-2.5 border cursor-pointer transition-all',
                        selectedRents.has(idx)
                          ? 'bg-emerald-500/10 border-emerald-500/30'
                          : 'bg-slate-700/20 border-slate-700/40 hover:border-slate-600'
                      )}
                    >
                      <div className={cn(
                        'mt-0.5 flex-shrink-0 w-4 h-4 rounded border flex items-center justify-center transition-colors',
                        selectedRents.has(idx)
                          ? 'bg-emerald-500 border-emerald-500'
                          : 'border-slate-600 bg-transparent'
                      )}>
                        {selectedRents.has(idx) && (
                          <svg className="w-3 h-3 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                            <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                          </svg>
                        )}
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between">
                          <span className="text-xs font-medium text-slate-200">{r.room_type || 'Room'}</span>
                          <div className="flex items-center gap-2">
                            <div className="w-12 h-1 bg-slate-700 rounded-full overflow-hidden">
                              <div
                                className={cn('h-full rounded-full', confidenceBar(r.confidence))}
                                style={{ width: `${r.confidence * 100}%` }}
                              />
                            </div>
                            <span className={cn('text-[10px] font-mono', confidenceColor(r.confidence))}>
                              {Math.round(r.confidence * 100)}%
                            </span>
                          </div>
                        </div>
                        <div className="mt-1 flex items-center gap-2 text-xs text-slate-300">
                          {r.rent_per_week != null && <span>£{r.rent_per_week}/wk</span>}
                          {r.rent_per_month != null && <span>£{r.rent_per_month}/mo</span>}
                          {r.academic_year && <span className="text-slate-500">· {r.academic_year}</span>}
                          {r.contract_length_weeks && <span className="text-slate-500">· {r.contract_length_weeks}wk tenancy</span>}
                        </div>
                        {r.reasoning && (
                          <p className="mt-1 text-[10px] text-slate-500 leading-relaxed">{r.reasoning}</p>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}

              {/* AI Notes */}
              {aiNotes && (
                <div className="bg-slate-700/20 rounded-lg p-3 border border-slate-700/40">
                  <p className="text-[11px] text-slate-400 leading-relaxed">
                    <span className="text-slate-500 font-medium">AI Notes: </span>
                    {aiNotes}
                  </p>
                </div>
              )}

              {/* Apply button */}
              <div className="flex justify-end pt-1">
                <button
                  onClick={handleApply}
                  disabled={(selected.size === 0 && selectedRents.size === 0) || applying}
                  className={cn(
                    'inline-flex items-center gap-2 px-4 py-2 text-sm font-medium rounded-lg transition-all',
                    (selected.size > 0 || selectedRents.size > 0)
                      ? 'text-white bg-gradient-to-r from-amber-600 to-orange-600 hover:from-amber-500 hover:to-orange-500 shadow-lg shadow-amber-500/20'
                      : 'text-slate-500 bg-slate-700/50 cursor-not-allowed'
                  )}
                >
                  {applying ? (
                    <>
                      <ArrowPathIcon className="w-4 h-4 animate-spin" />
                      Applying...
                    </>
                  ) : (
                    <>
                      <CheckCircleIcon className="w-4 h-4" />
                      Apply {selected.size}{selectedRents.size > 0 ? `+${selectedRents.size} rents` : ''}
                    </>
                  )}
                </button>
              </div>
            </>
          )}

          {/* No suggestions */}
          {suggestions && suggestions.length === 0 && !applied && (
            <div className="text-center py-4">
              <p className="text-xs text-slate-500">No additional suggestions found for this scheme.</p>
              <p className="text-[11px] text-slate-600 mt-1">All known fields appear to be populated.</p>
            </div>
          )}

          {/* Applied state */}
          {applied && (
            <div className="flex items-center gap-2 bg-emerald-500/10 border border-emerald-500/30 rounded-lg p-3">
              <CheckCircleIcon className="w-5 h-5 text-emerald-400 flex-shrink-0" />
              <div>
                <p className="text-xs text-emerald-300 font-medium">Suggestions applied successfully</p>
                <p className="text-[11px] text-slate-400 mt-0.5">
                  Scheme data has been updated. Refresh the page to see changes.
                </p>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
