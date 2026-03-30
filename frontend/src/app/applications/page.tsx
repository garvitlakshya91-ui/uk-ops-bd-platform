'use client';

import React, { useState, useEffect } from 'react';
import { ChevronDownIcon, ChevronUpIcon, MapIcon, DocumentMagnifyingGlassIcon, DocumentArrowDownIcon, BellIcon, BuildingOffice2Icon } from '@heroicons/react/24/outline';
import { cn, formatDate, getStatusColor, getSchemeTypeColor, getBdScoreColor } from '@/lib/utils';
import Card from '@/components/ui/Card';
import Badge from '@/components/ui/Badge';
import SearchInput from '@/components/ui/SearchInput';
import Select from '@/components/ui/Select';
import PermissionGate from '@/components/rbac/PermissionGate';
import api from '@/lib/api';

interface ApplicationRow {
  id: string;
  reference: string;
  address: string;
  postcode: string;
  council: string;
  type: string;
  units: number;
  status: string;
  applicant: string;
  date: string;
  bd_score: number;
  description: string;
  case_officer: string;
  decision_date: string;
}

const defaultCouncils = [
  'Manchester City Council', 'Leeds City Council', 'Bristol City Council', 'Birmingham City Council',
  'Liverpool City Council', 'Sheffield City Council', 'Nottingham City Council', 'City of Westminster',
  'Cardiff Council', 'Salford City Council', 'City of Edinburgh Council', 'Glasgow City Council',
  'Southampton City Council',
];

function getDaysSince(dateStr: string): number {
  if (!dateStr) return 0;
  const now = new Date();
  const d = new Date(dateStr);
  if (isNaN(d.getTime())) return 0;
  return Math.floor((now.getTime() - d.getTime()) / (1000 * 60 * 60 * 24));
}

function getStatusDot(status: string): string {
  const dots: Record<string, string> = {
    submitted: 'bg-yellow-400',
    validated: 'bg-blue-400',
    approved: 'bg-emerald-400',
    refused: 'bg-red-400',
  };
  return dots[(status || '').toLowerCase()] || 'bg-slate-400';
}

function getBdScoreBg(score: number): string {
  if (score >= 85) return 'bg-red-500/20 text-red-400 border-red-500/40';
  if (score >= 70) return 'bg-amber-500/20 text-amber-400 border-amber-500/40';
  return 'bg-green-500/20 text-green-400 border-green-500/40';
}

export default function ApplicationsPage() {
  const [applications, setApplications] = useState<ApplicationRow[]>([]);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [councilFilter, setCouncilFilter] = useState('');
  const [typeFilter, setTypeFilter] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [showMap, setShowMap] = useState(false);
  const [page, setPage] = useState(0);
  const [allCouncils, setAllCouncils] = useState<string[]>([]);
  const [sortBy, setSortBy] = useState<'date' | 'units' | 'bd_score' | ''>('');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const PAGE_SIZE = 100;

  function handleSort(col: 'date' | 'units' | 'bd_score') {
    if (sortBy === col) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(col);
      setSortDir('desc');
    }
  }

  function sortIcon(col: string) {
    if (sortBy !== col) return '↕';
    return sortDir === 'asc' ? '↑' : '↓';
  }

  // Fetch council list once (all councils with data)
  useEffect(() => {
    api.get('/v2/application-councils')
      .then(res => setAllCouncils(res.data || []))
      .catch(() => {});
  }, []);

  useEffect(() => {
    setLoading(true);
    const params: Record<string, string | number> = { limit: PAGE_SIZE, skip: page * PAGE_SIZE };
    if (councilFilter) params.council = councilFilter;
    if (typeFilter) params.scheme_type = typeFilter;
    if (statusFilter) params.status = statusFilter;
    if (search) params.search = search;
    if (sortBy) { params.sort_by = sortBy; params.sort_dir = sortDir; }

    api.get('/v2/applications', { params })
      .then(res => {
        const data = res.data;
        const items: ApplicationRow[] = (Array.isArray(data) ? data : data?.items || []).map((a: any) => ({
          id: a.id || '',
          reference: a.reference || '',
          address: a.address || '',
          postcode: a.postcode || '',
          council: a.council || '',
          type: a.scheme_type || a.type || 'Unknown',
          units: a.units ?? 0,
          status: a.status || 'Unknown',
          applicant: a.applicant || '',
          date: a.date || '',
          bd_score: a.bd_score ?? 0,
          description: a.description || '',
          case_officer: a.case_officer || '',
          decision_date: a.decision_date || '',
        }));
        setApplications(items);
        setTotal(data?.total || items.length);
        setLoading(false);
      })
      .catch(() => {
        setLoading(false);
      });
  }, [councilFilter, typeFilter, statusFilter, search, page, sortBy, sortDir]);

  function getSimilarApplications(app: ApplicationRow): ApplicationRow[] {
    return applications
      .filter((a) => a.type === app.type && a.id !== app.id)
      .slice(0, 2);
  }

  // Filtering is done server-side now via API params
  const filtered = applications;

  const maxUnits = filtered.length > 0 ? Math.max(...filtered.map((a) => a.units || 0), 1) : 1;
  const totalPages = Math.ceil(total / PAGE_SIZE);

  const btrCount = applications.filter((a) => a.type === 'BTR').length;
  const pbsaCount = applications.filter((a) => a.type === 'PBSA').length;
  const approvedCount = applications.filter((a) => a.status === 'Approved').length;

  const councilOptions = allCouncils.length > 0 ? allCouncils : defaultCouncils;

  if (loading && applications.length === 0) {
    return (
      <div className="space-y-6">
        <div className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-blue-600 via-indigo-600 to-violet-600 p-8">
          <div className="relative">
            <h1 className="text-3xl font-bold text-white tracking-tight">Planning Intelligence</h1>
            <p className="text-blue-100 mt-1 text-sm">Loading applications...</p>
          </div>
        </div>
        <div className="grid grid-cols-1 gap-4">
          {[1, 2, 3, 4, 5].map((i) => (
            <div key={i} className="bg-slate-800/80 border border-slate-700/50 rounded-xl p-4 animate-pulse">
              <div className="h-4 bg-slate-700 rounded w-full mb-2" />
              <div className="h-3 bg-slate-700 rounded w-3/4" />
            </div>
          ))}
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Gradient Header */}
      <div className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-blue-600 via-indigo-600 to-violet-600 p-8">
        <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHZpZXdCb3g9IjAgMCA2MCA2MCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48ZyBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPjxnIGZpbGw9IiNmZmYiIGZpbGwtb3BhY2l0eT0iMC4wNSI+PGNpcmNsZSBjeD0iMzAiIGN5PSIzMCIgcj0iMiIvPjwvZz48L2c+PC9zdmc+')] opacity-40" />
        <div className="relative">
          <h1 className="text-3xl font-bold text-white tracking-tight">Planning Intelligence</h1>
          <p className="text-blue-100 mt-1 text-sm">Real-time planning application monitoring across the UK</p>
        </div>
        {/* Summary Stats */}
        <div className="relative flex flex-wrap items-center gap-2 mt-5">
          <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-white/15 backdrop-blur-sm text-white text-sm font-semibold border border-white/20">
            Total: {total.toLocaleString()}
          </span>
          <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-purple-400/20 backdrop-blur-sm text-purple-100 text-sm font-medium border border-purple-300/20">
            <span className="w-2 h-2 rounded-full bg-purple-400" /> BTR {btrCount}
          </span>
          <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-blue-400/20 backdrop-blur-sm text-blue-100 text-sm font-medium border border-blue-300/20">
            <span className="w-2 h-2 rounded-full bg-blue-400" /> PBSA {pbsaCount}
          </span>
          <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-emerald-400/20 backdrop-blur-sm text-emerald-100 text-sm font-medium border border-emerald-300/20">
            <span className="w-2 h-2 rounded-full bg-emerald-400" /> Approved {approvedCount}
          </span>
          <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-orange-400/20 backdrop-blur-sm text-orange-100 text-sm font-medium border border-orange-300/20">
            <span className="w-2 h-2 rounded-full bg-orange-400" /> New This Week {applications.filter((a) => {
              if (!a.date) return false;
              const d = new Date(a.date);
              if (isNaN(d.getTime())) return false;
              const now = new Date();
              const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
              return d >= weekAgo;
            }).length}
          </span>
        </div>
      </div>

      {/* Filters */}
      <div className="flex flex-wrap items-center gap-3">
        <SearchInput
          placeholder="Search reference, address, applicant..."
          onChange={setSearch}
          className="w-80"
        />
        <Select
          options={councilOptions.map((c) => ({ value: c, label: c }))}
          value={councilFilter}
          onChange={setCouncilFilter}
          placeholder="All Councils"
          className="w-56"
        />
        <Select
          options={[
            { value: 'BTR', label: 'BTR' },
            { value: 'PBSA', label: 'PBSA' },
            { value: 'Co-living', label: 'Co-living' },
            { value: 'Senior', label: 'Senior Living' },
            { value: 'Affordable', label: 'Affordable' },
            { value: 'Mixed', label: 'Mixed Use' },
            { value: 'Residential', label: 'Residential (50+)' },
          ]}
          value={typeFilter}
          onChange={setTypeFilter}
          placeholder="All Scheme Types"
          className="w-44"
        />
        <Select
          options={[
            { value: 'Permissioned', label: 'Permissioned' },
            { value: 'Approved', label: 'Approved' },
            { value: 'Allocated', label: 'Allocated (No Permission)' },
            { value: 'Pending Decision', label: 'Pending Decision' },
            { value: 'Refused', label: 'Refused' },
            { value: 'Withdrawn', label: 'Withdrawn' },
          ]}
          value={statusFilter}
          onChange={setStatusFilter}
          placeholder="All Statuses"
          className="w-40"
        />
        <button
          onClick={() => setShowMap(!showMap)}
          className={cn(
            'inline-flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-all border',
            showMap
              ? 'bg-blue-500/20 text-blue-400 border-blue-500/30'
              : 'bg-slate-700/50 text-slate-300 border-slate-600 hover:bg-slate-700 hover:text-white'
          )}
        >
          <MapIcon className="w-4 h-4" />
          Map View
        </button>
        {(search || councilFilter || typeFilter || statusFilter) && (
          <button
            onClick={() => { setSearch(''); setCouncilFilter(''); setTypeFilter(''); setStatusFilter(''); }}
            className="text-xs text-slate-400 hover:text-white px-2 py-1"
          >
            Clear filters
          </button>
        )}
        <span className="ml-auto text-sm text-slate-500">{filtered.length} applications</span>
      </div>

      {/* Map Placeholder */}
      {showMap && (
        <Card>
          <div className="flex items-center justify-center h-64 border-2 border-dashed border-slate-600 rounded-xl">
            <div className="text-center">
              <MapIcon className="w-12 h-12 mx-auto text-slate-500 mb-3" />
              <p className="text-slate-400 font-medium">Map view coming soon</p>
              <p className="text-slate-500 text-sm mt-1">Interactive map of all planning applications</p>
            </div>
          </div>
        </Card>
      )}

      {/* Table */}
      <Card noPadding>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700">
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase w-8"></th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Reference</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Address</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Council</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Type</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase cursor-pointer hover:text-white select-none" onClick={() => handleSort('units')}>Units {sortIcon('units')}</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Status</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Applicant</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase cursor-pointer hover:text-white select-none" onClick={() => handleSort('date')}>Date {sortIcon('date')}</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase cursor-pointer hover:text-white select-none" onClick={() => handleSort('bd_score')}>BD Score {sortIcon('bd_score')}</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {filtered.map((app) => {
                const daysSince = getDaysSince(app.date);
                const unitBarWidth = Math.round((app.units / maxUnits) * 100);
                return (
                  <React.Fragment key={app.id}>
                    <tr
                      id={`app-row-${app.id}`}
                      className="hover:bg-slate-700/50 transition-colors cursor-pointer"
                      onClick={() => setExpandedId(expandedId === app.id ? null : app.id)}
                    >
                      <td className="px-4 py-3">
                        {expandedId === app.id ? (
                          <ChevronUpIcon className="w-4 h-4 text-slate-400" />
                        ) : (
                          <ChevronDownIcon className="w-4 h-4 text-slate-400" />
                        )}
                      </td>
                      <td className="px-4 py-3">
                        <span className="inline-flex items-center px-2.5 py-1 rounded-md bg-blue-500/10 border border-blue-500/20 font-mono text-xs text-blue-400 font-medium">
                          {app.reference}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-slate-300 max-w-[220px] truncate">{app.address}</td>
                      <td className="px-4 py-3 text-slate-400 text-xs">{app.council}</td>
                      <td className="px-4 py-3"><Badge variant={getSchemeTypeColor(app.type)}>{app.type}</Badge></td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-2">
                          <span className="text-slate-300 text-xs font-medium w-8">{app.units}</span>
                          <div className="w-16 h-2 bg-slate-700 rounded-full overflow-hidden">
                            <div
                              className="h-full bg-gradient-to-r from-indigo-500 to-violet-500 rounded-full"
                              style={{ width: `${unitBarWidth}%` }}
                            />
                          </div>
                        </div>
                      </td>
                      <td className="px-4 py-3">
                        <Badge variant={getStatusColor((app.status || '').toLowerCase())} dot dotColor={getStatusDot(app.status)}>
                          {app.status}
                        </Badge>
                      </td>
                      <td className="px-4 py-3 text-slate-400 text-xs max-w-[160px] truncate">{app.applicant}</td>
                      <td className="px-4 py-3">
                        <div>
                          <span className="text-slate-400 text-xs">{formatDate(app.date)}</span>
                          <span className="block text-[10px] text-slate-500 mt-0.5">{daysSince}d ago</span>
                        </div>
                      </td>
                      <td className="px-4 py-3">
                        <span className={cn(
                          'inline-flex items-center justify-center w-9 h-9 rounded-full text-xs font-bold border',
                          getBdScoreBg(app.bd_score)
                        )}>
                          {app.bd_score}
                        </span>
                      </td>
                    </tr>
                    {expandedId === app.id && (
                      <tr>
                        <td colSpan={10} className="px-8 py-6 bg-slate-800/50">
                          {/* Two column layout */}
                          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                            {/* Left - Description */}
                            <div>
                              <h4 className="text-sm font-semibold text-white mb-2">Description</h4>
                              <p className="text-sm text-slate-400 leading-relaxed">
                                {(app.description || '').split(app.type).map((part, i, arr) =>
                                  i < arr.length - 1 ? (
                                    <React.Fragment key={i}>
                                      {part}
                                      <span className="inline-flex items-center px-1.5 py-0.5 rounded bg-indigo-500/20 text-indigo-300 text-xs font-semibold mx-0.5">
                                        {app.type}
                                      </span>
                                    </React.Fragment>
                                  ) : (
                                    <React.Fragment key={i}>{part}</React.Fragment>
                                  )
                                )}
                              </p>

                              {/* Similar Applications */}
                              <div className="mt-5">
                                <h4 className="text-sm font-semibold text-white mb-2">Similar Applications</h4>
                                <div className="space-y-2">
                                  {getSimilarApplications(app).map((sim) => (
                                    <div
                                      key={sim.id}
                                      className="bg-slate-700/30 border border-slate-600/30 rounded-lg p-3 flex items-center justify-between cursor-pointer hover:bg-slate-700/50 transition-colors"
                                      onClick={(e) => {
                                        e.stopPropagation();
                                        setExpandedId(sim.id);
                                        setTimeout(() => {
                                          document.getElementById(`app-row-${sim.id}`)?.scrollIntoView({ behavior: 'smooth', block: 'center' });
                                        }, 100);
                                      }}
                                    >
                                      <div>
                                        <span className="font-mono text-xs text-blue-400">{sim.reference}</span>
                                        <p className="text-xs text-slate-400 mt-0.5">{sim.address}</p>
                                      </div>
                                      <div className="flex items-center gap-2">
                                        <Badge variant={getSchemeTypeColor(sim.type)} size="sm">{sim.type}</Badge>
                                        <span className="text-xs text-slate-500">{sim.units} units</span>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </div>

                              {/* Applicant Profile */}
                              <div className="mt-5">
                                <h4 className="text-sm font-semibold text-white mb-2">Applicant Profile</h4>
                                <div className="bg-slate-700/30 border border-slate-600/30 rounded-lg p-3 flex items-center gap-3">
                                  <div className="w-10 h-10 rounded-full bg-indigo-500/20 border border-indigo-500/30 flex items-center justify-center flex-shrink-0">
                                    <BuildingOffice2Icon className="w-5 h-5 text-indigo-400" />
                                  </div>
                                  <div className="flex-1 min-w-0">
                                    <p className="text-sm font-medium text-white truncate">{app.applicant}</p>
                                    <a href="/companies" className="text-xs text-blue-400 hover:text-blue-300 hover:underline">
                                      View in Companies
                                    </a>
                                  </div>
                                </div>
                              </div>
                            </div>

                            {/* Right - Metadata Grid */}
                            <div className="space-y-4">
                              <h4 className="text-sm font-semibold text-white">Details</h4>
                              <div className="grid grid-cols-2 gap-y-3 gap-x-4 text-sm bg-slate-700/20 rounded-xl p-4 border border-slate-600/20">
                                <span className="text-slate-500">Case Officer</span>
                                <span className="text-slate-300">{app.case_officer}</span>
                                <span className="text-slate-500">Target Decision</span>
                                <span className="text-slate-300">{formatDate(app.decision_date)}</span>
                                <span className="text-slate-500">Applicant</span>
                                <span className="text-slate-300">{app.applicant}</span>
                                <span className="text-slate-500">Full Address</span>
                                <span className="text-slate-300">{app.address || '--'}</span>
                                <span className="text-slate-500">Postcode</span>
                                <span className="text-slate-300 font-mono">{app.postcode || '--'}</span>
                                <span className="text-slate-500">Days Since Filed</span>
                                <span className="text-slate-300">{getDaysSince(app.date)} days</span>
                                <span className="text-slate-500">Scheme Type</span>
                                <span><Badge variant={getSchemeTypeColor(app.type)} size="sm">{app.type}</Badge></span>
                              </div>

                              {/* Action Buttons */}
                              <div className="flex flex-wrap gap-2 mt-4">
                                <PermissionGate resource="pipeline" action="create">
                                  <button className="inline-flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-white bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors">
                                    <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" strokeWidth={2} stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" d="M12 4.5v15m7.5-7.5h-15" /></svg>
                                    Add to Pipeline
                                  </button>
                                </PermissionGate>
                                <button className="inline-flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-slate-300 bg-slate-700 hover:bg-slate-600 rounded-lg transition-colors border border-slate-600">
                                  <DocumentArrowDownIcon className="w-4 h-4" />
                                  View Documents
                                </button>
                                <button className="inline-flex items-center gap-1.5 px-4 py-2 text-sm font-medium text-slate-300 bg-slate-700 hover:bg-slate-600 rounded-lg transition-colors border border-slate-600">
                                  <BellIcon className="w-4 h-4" />
                                  Track Changes
                                </button>
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
              {filtered.length === 0 && (
                <tr>
                  <td colSpan={10} className="px-4 py-12 text-center text-slate-500">
                    No applications match your filters
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between px-2">
          <p className="text-sm text-slate-400">
            Showing {page * PAGE_SIZE + 1}–{Math.min((page + 1) * PAGE_SIZE, total)} of {total.toLocaleString()} applications
          </p>
          <div className="flex items-center gap-2">
            <button
              onClick={() => setPage(Math.max(0, page - 1))}
              disabled={page === 0}
              className="px-3 py-1.5 rounded-lg bg-slate-700/50 text-slate-300 text-sm border border-slate-600/50 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Previous
            </button>
            <span className="text-sm text-slate-400">
              Page {page + 1} of {totalPages}
            </span>
            <button
              onClick={() => setPage(Math.min(totalPages - 1, page + 1))}
              disabled={page >= totalPages - 1}
              className="px-3 py-1.5 rounded-lg bg-slate-700/50 text-slate-300 text-sm border border-slate-600/50 hover:bg-slate-700 disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
