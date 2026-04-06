'use client';

import React, { useState, useEffect } from 'react';
import {
  ChevronDownIcon,
  ChevronUpIcon,
  DocumentDuplicateIcon,
  ArrowsUpDownIcon,
  CurrencyPoundIcon,
  ClockIcon,
  ExclamationTriangleIcon,
  CheckCircleIcon,
  XMarkIcon,
  FunnelIcon,
} from '@heroicons/react/24/outline';
import { cn, formatDate, formatCurrency, formatNumber, getContractEndColor } from '@/lib/utils';
import Card from '@/components/ui/Card';
import Badge from '@/components/ui/Badge';
import SearchInput from '@/components/ui/SearchInput';
import Select from '@/components/ui/Select';
import api from '@/lib/api';

/* ------------------------------------------------------------------ */
/* Types                                                               */
/* ------------------------------------------------------------------ */
interface ContractRow {
  id: string;
  contract_reference: string | null;
  contract_type: string | null;
  scheme_name: string | null;
  scheme_id: string | null;
  operator: string | null;
  client: string | null;
  contract_start: string | null;
  contract_end: string | null;
  contract_value: number | null;
  currency: string | null;
  source: string | null;
  source_reference: string | null;
  is_current: boolean | null;
  scheme_type: string | null;
  scheme_postcode: string | null;
  scheme_council: string | null;
  created_at: string | null;
}

interface ContractStatsData {
  total: number;
  current: number;
  expired: number;
  upcoming: number;
  expiring_6m: number;
  total_value: number;
  avg_value: number;
  type_distribution: Record<string, number>;
  source_distribution: Record<string, number>;
}

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */
function getContractStatus(row: ContractRow): { label: string; variant: string } {
  const now = new Date();
  const end = row.contract_end ? new Date(row.contract_end) : null;
  const start = row.contract_start ? new Date(row.contract_start) : null;

  if (start && start > now) {
    return { label: 'Upcoming', variant: 'bg-blue-500/20 text-blue-400 border-blue-500/30' };
  }
  if (end && end < now) {
    return { label: 'Expired', variant: 'bg-red-500/20 text-red-400 border-red-500/30' };
  }
  if (row.is_current || (start && start <= now && end && end >= now)) {
    return { label: 'Current', variant: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/30' };
  }
  return { label: 'Unknown', variant: 'bg-slate-500/20 text-slate-400 border-slate-500/30' };
}

function formatSourceLabel(source: string | null): string {
  if (!source) return '--';
  const labels: Record<string, string> = {
    find_a_tender: 'Find a Tender',
    contracts_finder: 'Contracts Finder',
    manual: 'Manual',
  };
  return labels[source] || source;
}

function getDaysRemaining(dateStr: string): number | null {
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return null;
  return Math.ceil((d.getTime() - Date.now()) / (1000 * 60 * 60 * 24));
}

function getDaysRemainingColor(days: number | null): string {
  if (days === null) return 'text-slate-500';
  if (days < 0) return 'text-red-400';
  if (days < 180) return 'text-red-400';
  if (days < 365) return 'text-amber-400';
  return 'text-slate-400';
}

function getContractTypeColor(type: string | null): string {
  if (!type) return 'bg-slate-500/20 text-slate-300 border-slate-500/30';
  const t = type.toLowerCase();
  if (t.includes('maintenance') || t.includes('repair')) return 'bg-orange-500/20 text-orange-300 border-orange-500/30';
  if (t.includes('management')) return 'bg-blue-500/20 text-blue-300 border-blue-500/30';
  if (t.includes('affordable')) return 'bg-emerald-500/20 text-emerald-300 border-emerald-500/30';
  if (t.includes('senior')) return 'bg-purple-500/20 text-purple-300 border-purple-500/30';
  if (t.includes('supported')) return 'bg-cyan-500/20 text-cyan-300 border-cyan-500/30';
  if (t.includes('btr')) return 'bg-violet-500/20 text-violet-300 border-violet-500/30';
  if (t.includes('pbsa')) return 'bg-pink-500/20 text-pink-300 border-pink-500/30';
  if (t.includes('facilities')) return 'bg-amber-500/20 text-amber-300 border-amber-500/30';
  if (t.includes('care')) return 'bg-rose-500/20 text-rose-300 border-rose-500/30';
  return 'bg-slate-500/20 text-slate-300 border-slate-500/30';
}

/* ------------------------------------------------------------------ */
/* Value bar (mini progress)                                           */
/* ------------------------------------------------------------------ */
function ValueBar({ value, max }: { value: number | null; max: number }) {
  if (!value || value <= 0) return <span className="text-slate-500 text-xs">--</span>;
  const pct = Math.min(100, (value / max) * 100);
  return (
    <div className="flex items-center gap-2">
      <span className="text-xs font-medium text-slate-200 whitespace-nowrap">{formatCurrency(value)}</span>
      <div className="w-16 h-1.5 bg-slate-700 rounded-full overflow-hidden hidden xl:block">
        <div
          className="h-full bg-gradient-to-r from-blue-500 to-cyan-400 rounded-full"
          style={{ width: `${pct}%` }}
        />
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Page                                                                */
/* ------------------------------------------------------------------ */
export default function ContractsPage() {
  const [contracts, setContracts] = useState<ContractRow[]>([]);
  const [stats, setStats] = useState<ContractStatsData | null>(null);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [sourceFilter, setSourceFilter] = useState('');
  const [typeFilter, setTypeFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [sortBy, setSortBy] = useState('');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const limit = 50;

  // Fetch stats once
  useEffect(() => {
    api.get('/v2/contracts/stats')
      .then(res => setStats(res.data))
      .catch(() => {});
  }, []);

  useEffect(() => {
    setPage(0);
  }, [search, sourceFilter, typeFilter, statusFilter, sortBy, sortDir]);

  useEffect(() => {
    const fetchContracts = async () => {
      setLoading(true);
      try {
        const params: Record<string, string | number> = {
          skip: page * limit,
          limit,
        };
        if (search) params.search = search;
        if (sourceFilter) params.source = sourceFilter;
        if (typeFilter) params.contract_type = typeFilter;
        if (statusFilter) params.status = statusFilter;
        if (sortBy) {
          params.sort_by = sortBy;
          params.sort_dir = sortDir;
        }
        const { data } = await api.get('/v2/contracts', { params });
        setContracts(data.items);
        setTotal(data.total);
      } catch (err) {
        console.error('Failed to load contracts:', err);
      } finally {
        setLoading(false);
      }
    };
    fetchContracts();
  }, [search, sourceFilter, typeFilter, statusFilter, sortBy, sortDir, page]);

  const toggleSort = (field: string) => {
    if (sortBy === field) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(field);
      setSortDir('desc');
    }
  };

  const totalPages = Math.ceil(total / limit);
  const hasFilters = search || sourceFilter || typeFilter || statusFilter;

  const typeOptions = stats
    ? Object.entries(stats.type_distribution)
        .sort((a, b) => b[1] - a[1])
        .slice(0, 15)
        .map(([k, v]) => ({ value: k, label: `${k} (${v})` }))
    : [];

  const maxValue = contracts.reduce((mx, c) => Math.max(mx, c.contract_value ?? 0), 1);

  const SortIcon = ({ field }: { field: string }) => {
    if (sortBy !== field) return <ArrowsUpDownIcon className="w-3.5 h-3.5 text-slate-500" />;
    return sortDir === 'asc'
      ? <ChevronUpIcon className="w-3.5 h-3.5 text-blue-400" />
      : <ChevronDownIcon className="w-3.5 h-3.5 text-blue-400" />;
  };

  /* ---------------------------------------------------------------- */
  /* Loading skeleton                                                  */
  /* ---------------------------------------------------------------- */
  if (!stats) {
    return (
      <div className="space-y-6">
        <div className="h-8 bg-slate-800 rounded w-64 animate-pulse" />
        <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
          {[1, 2, 3, 4].map((i) => (
            <div key={i} className="bg-slate-800 border border-slate-700 rounded-xl p-5 animate-pulse">
              <div className="h-3 bg-slate-700 rounded w-24 mb-3" />
              <div className="h-8 bg-slate-700 rounded w-16" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  /* ---------------------------------------------------------------- */
  /* Render                                                            */
  /* ---------------------------------------------------------------- */
  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-2xl font-bold bg-gradient-to-r from-cyan-400 via-blue-400 to-indigo-400 bg-clip-text text-transparent">
          Contract Intelligence
        </h1>
        <p className="text-sm text-slate-400 mt-1">
          Track contract lifecycle, values, and expiry across {stats.total.toLocaleString()} contracts
        </p>
      </div>

      {/* Summary stats */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div className="bg-slate-800 border border-slate-700 rounded-xl p-5 shadow-lg border-l-4 border-l-blue-500">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Total Contracts</p>
              <p className="mt-2 text-3xl font-bold text-white">{stats.total.toLocaleString()}</p>
            </div>
            <div className="p-2.5 bg-blue-500/10 rounded-lg">
              <DocumentDuplicateIcon className="w-5 h-5 text-blue-400" />
            </div>
          </div>
        </div>

        <div className="bg-slate-800 border border-slate-700 rounded-xl p-5 shadow-lg border-l-4 border-l-emerald-500">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Total Value</p>
              <p className="mt-2 text-3xl font-bold text-white">£{formatNumber(stats.total_value)}</p>
            </div>
            <div className="p-2.5 bg-emerald-500/10 rounded-lg">
              <CurrencyPoundIcon className="w-5 h-5 text-emerald-400" />
            </div>
          </div>
        </div>

        <div className="bg-slate-800 border border-slate-700 rounded-xl p-5 shadow-lg border-l-4 border-l-violet-500">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Active Contracts</p>
              <p className="mt-2 text-3xl font-bold text-white">{stats.current.toLocaleString()}</p>
            </div>
            <div className="p-2.5 bg-violet-500/10 rounded-lg">
              <CheckCircleIcon className="w-5 h-5 text-violet-400" />
            </div>
          </div>
        </div>

        <div className="bg-slate-800 border border-slate-700 rounded-xl p-5 shadow-lg border-l-4 border-l-red-500">
          <div className="flex items-start justify-between">
            <div>
              <p className="text-xs font-medium text-slate-400 uppercase tracking-wider">Expiring &lt;6 Months</p>
              <p className="mt-2 text-3xl font-bold text-white">{stats.expiring_6m}</p>
            </div>
            <div className="p-2.5 bg-red-500/10 rounded-lg relative">
              <ExclamationTriangleIcon className="w-5 h-5 text-red-400" />
              {stats.expiring_6m > 0 && (
                <span className="absolute -top-0.5 -right-0.5 w-2.5 h-2.5 bg-red-500 rounded-full animate-pulse" />
              )}
            </div>
          </div>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <SearchInput
          placeholder="Search contracts, schemes, operators, clients..."
          onChange={setSearch}
          className="w-72"
        />
        <Select
          options={[
            { value: 'value', label: 'Sort by Value' },
            { value: 'start_date', label: 'Sort by Start Date' },
            { value: 'end_date', label: 'Sort by End Date' },
          ]}
          value={sortBy}
          onChange={setSortBy}
          placeholder="Sort by..."
          className="w-48"
        />
        <Select
          value={sourceFilter}
          onChange={setSourceFilter}
          placeholder="All Sources"
          options={[
            { value: 'find_a_tender', label: 'Find a Tender' },
            { value: 'contracts_finder', label: 'Contracts Finder' },
          ]}
          className="w-48"
        />
        <Select
          value={typeFilter}
          onChange={setTypeFilter}
          placeholder="All Types"
          options={typeOptions}
          className="w-52"
        />
        <Select
          value={statusFilter}
          onChange={setStatusFilter}
          placeholder="All Statuses"
          options={[
            { value: 'current', label: 'Current' },
            { value: 'expired', label: 'Expired' },
            { value: 'upcoming', label: 'Upcoming' },
            { value: 'expiring', label: 'Expiring <6m' },
          ]}
          className="w-44"
        />
        {hasFilters && (
          <button
            onClick={() => { setSearch(''); setSourceFilter(''); setTypeFilter(''); setStatusFilter(''); setSortBy(''); }}
            className="text-xs text-slate-400 hover:text-white px-2 py-1 flex items-center gap-1"
          >
            <XMarkIcon className="w-3.5 h-3.5" /> Clear filters
          </button>
        )}
      </div>

      {/* Results count */}
      {hasFilters && (
        <p className="text-xs text-slate-500">
          <FunnelIcon className="w-3.5 h-3.5 inline mr-1" />
          Showing {total.toLocaleString()} contract{total !== 1 ? 's' : ''} matching filters
        </p>
      )}

      {/* Table */}
      <Card noPadding>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700">
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase w-8"></th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Scheme</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Type</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Client</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase cursor-pointer select-none" onClick={() => toggleSort('value')}>
                  <span className="flex items-center gap-1">Value <SortIcon field="value" /></span>
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase cursor-pointer select-none" onClick={() => toggleSort('start_date')}>
                  <span className="flex items-center gap-1">Start <SortIcon field="start_date" /></span>
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase cursor-pointer select-none" onClick={() => toggleSort('end_date')}>
                  <span className="flex items-center gap-1">End <SortIcon field="end_date" /></span>
                </th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Days Left</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Source</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Status</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {loading ? (
                Array.from({ length: 10 }).map((_, i) => (
                  <tr key={i}>
                    {Array.from({ length: 10 }).map((_, j) => (
                      <td key={j} className="px-4 py-3">
                        <div className="h-4 bg-slate-700/50 rounded animate-pulse" />
                      </td>
                    ))}
                  </tr>
                ))
              ) : contracts.length === 0 ? (
                <tr>
                  <td colSpan={10} className="px-4 py-12 text-center text-slate-500">
                    No contracts found matching your criteria.
                  </td>
                </tr>
              ) : (
                contracts.map((contract) => {
                  const status = getContractStatus(contract);
                  const isExpanded = expandedId === contract.id;
                  const daysLeft = contract.contract_end ? getDaysRemaining(contract.contract_end) : null;
                  const daysColor = getDaysRemainingColor(daysLeft);

                  return (
                    <React.Fragment key={contract.id}>
                      <tr
                        className={cn(
                          'hover:bg-slate-700/50 transition-colors cursor-pointer',
                          isExpanded && 'bg-slate-800/30',
                          daysLeft !== null && daysLeft >= 0 && daysLeft < 180 && 'bg-gradient-to-r from-red-500/[0.04] to-transparent'
                        )}
                        onClick={() => setExpandedId(isExpanded ? null : contract.id)}
                      >
                        {/* Expand icon */}
                        <td className="px-4 py-3">
                          {isExpanded
                            ? <ChevronUpIcon className="w-4 h-4 text-slate-500" />
                            : <ChevronDownIcon className="w-4 h-4 text-slate-500" />
                          }
                        </td>

                        {/* Scheme name + council */}
                        <td className="px-4 py-3">
                          <div className="text-slate-200 font-medium truncate max-w-[220px]">
                            {contract.scheme_name || '--'}
                          </div>
                          <div className="text-[11px] text-slate-500 truncate max-w-[220px]">
                            {contract.scheme_council || contract.contract_reference || ''}
                          </div>
                        </td>

                        {/* Type badge */}
                        <td className="px-4 py-3">
                          {contract.contract_type ? (
                            <Badge variant={getContractTypeColor(contract.contract_type)} size="sm">
                              {contract.contract_type}
                            </Badge>
                          ) : (
                            <span className="text-slate-500 text-xs">--</span>
                          )}
                        </td>

                        {/* Client */}
                        <td className="px-4 py-3 text-slate-300 text-xs truncate max-w-[160px]">
                          {contract.client || '--'}
                        </td>

                        {/* Value with bar */}
                        <td className="px-4 py-3">
                          <ValueBar value={contract.contract_value} max={maxValue} />
                        </td>

                        {/* Start date */}
                        <td className="px-4 py-3 text-slate-400 text-xs whitespace-nowrap">
                          {contract.contract_start ? formatDate(contract.contract_start) : '--'}
                        </td>

                        {/* End date */}
                        <td className="px-4 py-3 whitespace-nowrap">
                          <span className={cn('text-xs', contract.contract_end ? getContractEndColor(contract.contract_end) : 'text-slate-500')}>
                            {contract.contract_end ? formatDate(contract.contract_end) : '--'}
                          </span>
                        </td>

                        {/* Days remaining */}
                        <td className="px-4 py-3 whitespace-nowrap">
                          {daysLeft !== null ? (
                            <span className={cn('text-xs font-medium', daysColor)}>
                              {daysLeft < 0 ? `${Math.abs(daysLeft)}d ago` : `${daysLeft}d`}
                            </span>
                          ) : (
                            <span className="text-slate-500 text-xs">--</span>
                          )}
                        </td>

                        {/* Source */}
                        <td className="px-4 py-3">
                          <span className="text-slate-500 text-[11px]">
                            {formatSourceLabel(contract.source)}
                          </span>
                        </td>

                        {/* Status badge */}
                        <td className="px-4 py-3">
                          <Badge variant={status.variant} size="sm">{status.label}</Badge>
                        </td>
                      </tr>

                      {/* Expanded details */}
                      {isExpanded && (
                        <tr className="bg-slate-800/20">
                          <td colSpan={10} className="px-6 py-5">
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                              {/* Contract Details */}
                              <div className="space-y-3">
                                <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2">
                                  <span className="w-1 h-3 bg-blue-500 rounded-full" />
                                  Contract Details
                                </h4>
                                <div className="space-y-2">
                                  <DetailRow label="Reference" value={contract.contract_reference} />
                                  <DetailRow label="Type" value={contract.contract_type} />
                                  <DetailRow
                                    label="Value"
                                    value={contract.contract_value != null
                                      ? `${formatCurrency(contract.contract_value)} ${contract.currency || 'GBP'}`
                                      : null}
                                  />
                                  <DetailRow label="Start Date" value={contract.contract_start ? formatDate(contract.contract_start) : null} />
                                  <DetailRow label="End Date" value={contract.contract_end ? formatDate(contract.contract_end) : null} />
                                  <DetailRow label="Current" value={contract.is_current != null ? (contract.is_current ? 'Yes' : 'No') : null} />
                                </div>
                              </div>

                              {/* Scheme Info */}
                              <div className="space-y-3">
                                <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2">
                                  <span className="w-1 h-3 bg-emerald-500 rounded-full" />
                                  Linked Scheme
                                </h4>
                                <div className="space-y-2">
                                  <DetailRow label="Scheme Name" value={contract.scheme_name} />
                                  <DetailRow label="Scheme Type" value={contract.scheme_type} />
                                  <DetailRow label="Postcode" value={contract.scheme_postcode} />
                                  <DetailRow label="Council" value={contract.scheme_council} />
                                </div>
                              </div>

                              {/* Source & Parties */}
                              <div className="space-y-3">
                                <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider flex items-center gap-2">
                                  <span className="w-1 h-3 bg-violet-500 rounded-full" />
                                  Source & Parties
                                </h4>
                                <div className="space-y-2">
                                  <DetailRow label="Operator" value={contract.operator} />
                                  <DetailRow label="Client" value={contract.client} />
                                  <DetailRow label="Source" value={formatSourceLabel(contract.source)} />
                                  <DetailRow label="Source Ref" value={contract.source_reference} />
                                  <DetailRow label="Added" value={contract.created_at ? formatDate(contract.created_at) : null} />
                                </div>
                              </div>
                            </div>
                          </td>
                        </tr>
                      )}
                    </React.Fragment>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        {/* Pagination */}
        {totalPages > 1 && (
          <div className="flex items-center justify-between px-4 py-3 border-t border-slate-700/50">
            <p className="text-xs text-slate-500">
              Showing {(page * limit + 1).toLocaleString()}–{Math.min((page + 1) * limit, total).toLocaleString()} of {total.toLocaleString()}
            </p>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setPage(0)}
                disabled={page === 0}
                className="px-2.5 py-1.5 text-xs font-medium rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                First
              </button>
              <button
                onClick={() => setPage(Math.max(0, page - 1))}
                disabled={page === 0}
                className="px-3 py-1.5 text-xs font-medium rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                Previous
              </button>
              <span className="text-xs text-slate-400">
                Page {page + 1} of {totalPages}
              </span>
              <button
                onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
                disabled={page >= totalPages - 1}
                className="px-3 py-1.5 text-xs font-medium rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                Next
              </button>
              <button
                onClick={() => setPage(totalPages - 1)}
                disabled={page >= totalPages - 1}
                className="px-2.5 py-1.5 text-xs font-medium rounded-lg bg-slate-700 text-slate-300 hover:bg-slate-600 disabled:opacity-40 disabled:cursor-not-allowed transition-colors"
              >
                Last
              </button>
            </div>
          </div>
        )}
      </Card>
    </div>
  );
}

function DetailRow({ label, value }: { label: string; value: string | null | undefined }) {
  return (
    <div className="flex items-start gap-2">
      <span className="text-xs text-slate-500 w-24 flex-shrink-0">{label}</span>
      <span className="text-xs text-slate-300">{value || '--'}</span>
    </div>
  );
}
