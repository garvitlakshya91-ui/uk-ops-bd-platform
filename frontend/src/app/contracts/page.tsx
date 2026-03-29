'use client';

import React, { useState, useEffect } from 'react';
import {
  ChevronDownIcon,
  ChevronUpIcon,
  DocumentDuplicateIcon,
  ArrowsUpDownIcon,
} from '@heroicons/react/24/outline';
import { cn, formatDate, formatCurrency } from '@/lib/utils';
import Card from '@/components/ui/Card';
import Badge from '@/components/ui/Badge';
import SearchInput from '@/components/ui/SearchInput';
import Select from '@/components/ui/Select';
import api from '@/lib/api';

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

export default function ContractsPage() {
  const [contracts, setContracts] = useState<ContractRow[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [sourceFilter, setSourceFilter] = useState('');
  const [typeFilter, setTypeFilter] = useState('');
  const [sortBy, setSortBy] = useState('');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [page, setPage] = useState(0);
  const limit = 50;

  useEffect(() => {
    setPage(0);
  }, [search, sourceFilter, typeFilter, sortBy, sortDir]);

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
  }, [search, sourceFilter, typeFilter, sortBy, sortDir, page]);

  const toggleSort = (field: string) => {
    if (sortBy === field) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(field);
      setSortDir('desc');
    }
  };

  const totalPages = Math.ceil(total / limit);

  const SortIcon = ({ field }: { field: string }) => {
    if (sortBy !== field) return <ArrowsUpDownIcon className="w-3.5 h-3.5 text-slate-500" />;
    return sortDir === 'asc'
      ? <ChevronUpIcon className="w-3.5 h-3.5 text-blue-400" />
      : <ChevronDownIcon className="w-3.5 h-3.5 text-blue-400" />;
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-2xl font-bold text-white flex items-center gap-2">
            <DocumentDuplicateIcon className="w-7 h-7 text-blue-400" />
            Contracts
          </h1>
          <p className="text-sm text-slate-400 mt-1">
            {total.toLocaleString()} contract{total !== 1 ? 's' : ''} found
          </p>
        </div>
      </div>

      {/* Filters */}
      <Card className="p-4">
        <div className="flex flex-col md:flex-row gap-3">
          <div className="flex-1">
            <SearchInput
              value={search}
              onChange={setSearch}
              placeholder="Search contracts, schemes, operators, clients..."
            />
          </div>
          <Select
            value={sourceFilter}
            onChange={setSourceFilter}
            placeholder="All Sources"
            options={[
              { value: 'find_a_tender', label: 'Find a Tender' },
              { value: 'contracts_finder', label: 'Contracts Finder' },
              { value: 'manual', label: 'Manual' },
            ]}
            className="w-full md:w-48"
          />
          <Select
            value={typeFilter}
            onChange={setTypeFilter}
            placeholder="All Types"
            options={[
              { value: 'management', label: 'Management' },
              { value: 'maintenance', label: 'Maintenance' },
              { value: 'facilities', label: 'Facilities' },
              { value: 'concession', label: 'Concession' },
            ]}
            className="w-full md:w-48"
          />
        </div>
      </Card>

      {/* Table */}
      <Card className="overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700/50">
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                  Contract Ref
                </th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                  Scheme Name
                </th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                  Type
                </th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                  Operator
                </th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                  Client
                </th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer select-none" onClick={() => toggleSort('value')}>
                  <span className="flex items-center gap-1">Value <SortIcon field="value" /></span>
                </th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer select-none" onClick={() => toggleSort('start_date')}>
                  <span className="flex items-center gap-1">Start <SortIcon field="start_date" /></span>
                </th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider cursor-pointer select-none" onClick={() => toggleSort('end_date')}>
                  <span className="flex items-center gap-1">End <SortIcon field="end_date" /></span>
                </th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                  Source
                </th>
                <th className="text-left px-4 py-3 text-xs font-semibold text-slate-400 uppercase tracking-wider">
                  Status
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-800/50">
              {loading ? (
                Array.from({ length: 8 }).map((_, i) => (
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

                  return (
                    <React.Fragment key={contract.id}>
                      <tr
                        className={cn(
                          'hover:bg-slate-800/40 transition-colors cursor-pointer',
                          isExpanded && 'bg-slate-800/30'
                        )}
                        onClick={() => setExpandedId(isExpanded ? null : contract.id)}
                      >
                        <td className="px-4 py-3 text-slate-200 font-medium">
                          <div className="flex items-center gap-2">
                            {isExpanded
                              ? <ChevronUpIcon className="w-4 h-4 text-slate-500 flex-shrink-0" />
                              : <ChevronDownIcon className="w-4 h-4 text-slate-500 flex-shrink-0" />
                            }
                            <span className="truncate max-w-[160px]">
                              {contract.contract_reference || '--'}
                            </span>
                          </div>
                        </td>
                        <td className="px-4 py-3 text-slate-300 truncate max-w-[200px]">
                          {contract.scheme_name || '--'}
                        </td>
                        <td className="px-4 py-3">
                          {contract.contract_type ? (
                            <Badge variant="bg-purple-500/20 text-purple-300 border-purple-500/30">
                              {contract.contract_type}
                            </Badge>
                          ) : (
                            <span className="text-slate-500">--</span>
                          )}
                        </td>
                        <td className="px-4 py-3 text-slate-300 truncate max-w-[160px]">
                          {contract.operator || '--'}
                        </td>
                        <td className="px-4 py-3 text-slate-300 truncate max-w-[160px]">
                          {contract.client || '--'}
                        </td>
                        <td className="px-4 py-3 text-slate-200 font-medium whitespace-nowrap">
                          {contract.contract_value != null
                            ? formatCurrency(contract.contract_value)
                            : '--'}
                        </td>
                        <td className="px-4 py-3 text-slate-400 whitespace-nowrap">
                          {contract.contract_start ? formatDate(contract.contract_start) : '--'}
                        </td>
                        <td className="px-4 py-3 text-slate-400 whitespace-nowrap">
                          {contract.contract_end ? formatDate(contract.contract_end) : '--'}
                        </td>
                        <td className="px-4 py-3">
                          <span className="text-slate-400 text-xs">
                            {formatSourceLabel(contract.source)}
                          </span>
                        </td>
                        <td className="px-4 py-3">
                          <Badge variant={status.variant}>{status.label}</Badge>
                        </td>
                      </tr>

                      {/* Expanded row */}
                      {isExpanded && (
                        <tr className="bg-slate-800/20">
                          <td colSpan={10} className="px-6 py-4">
                            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                              {/* Contract Details */}
                              <div className="space-y-3">
                                <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
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
                                <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
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
                                <h4 className="text-xs font-semibold text-slate-400 uppercase tracking-wider">
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
              Showing {page * limit + 1}--{Math.min((page + 1) * limit, total)} of {total}
            </p>
            <div className="flex items-center gap-2">
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
