'use client';

import React, { useState, useEffect } from 'react';
import { ChevronDownIcon, ChevronUpIcon, ExclamationTriangleIcon, ClipboardDocumentIcon, EnvelopeIcon, PhoneIcon } from '@heroicons/react/24/outline';
import { cn, formatDate, getSchemeTypeColor } from '@/lib/utils';
import Card from '@/components/ui/Card';
import Badge from '@/components/ui/Badge';
import SearchInput from '@/components/ui/SearchInput';
import Modal from '@/components/ui/Modal';
import toast from 'react-hot-toast';
import api from '@/lib/api';

interface CompanyRow {
  id: string;
  name: string;
  type: string;
  companies_house_number: string;
  applications_count: number;
  schemes_count: number;
  contacts_count: number;
  contacts: { id: string; name: string; role: string; email: string; phone: string }[];
  linked_applications: { id: string; reference: string; address: string; type: string; status: string; date: string }[];
  linked_schemes: { id: string; name: string; units: number; scheme_type: string }[];
  duplicates?: { id: string; name: string; companies_house_number: string; confidence: number }[];
}

/* Companies loaded from API */

const avatarColors = [
  'bg-blue-500/20 text-blue-400 border-blue-500/30',
  'bg-purple-500/20 text-purple-400 border-purple-500/30',
  'bg-emerald-500/20 text-emerald-400 border-emerald-500/30',
  'bg-amber-500/20 text-amber-400 border-amber-500/30',
  'bg-rose-500/20 text-rose-400 border-rose-500/30',
  'bg-cyan-500/20 text-cyan-400 border-cyan-500/30',
  'bg-indigo-500/20 text-indigo-400 border-indigo-500/30',
  'bg-teal-500/20 text-teal-400 border-teal-500/30',
  'bg-orange-500/20 text-orange-400 border-orange-500/30',
  'bg-violet-500/20 text-violet-400 border-violet-500/30',
];

function getAvatarColor(index: number): string {
  return avatarColors[index % avatarColors.length];
}

function getRelationshipStrength(company: CompanyRow): { label: string; color: string; width: string } {
  const score = (company.applications_count ?? 0) + (company.schemes_count ?? 0) * 2 + (company.contacts_count ?? 0);
  if (score >= 15) return { label: 'Strong', color: 'bg-emerald-500', width: 'w-full' };
  if (score >= 8) return { label: 'Medium', color: 'bg-amber-500', width: 'w-2/3' };
  return { label: 'Weak', color: 'bg-slate-500', width: 'w-1/3' };
}

function getRevenuePotential(company: CompanyRow): string {
  const schemes = company.linked_schemes || [];
  const totalUnits = schemes.reduce((sum, s) => sum + (s.units ?? 0), 0);
  if (totalUnits === 0) return 'N/A';
  // Rough estimate: 3k per unit per year
  const revenue = totalUnits * 3000;
  if (revenue >= 1000000) return `${(revenue / 1000000).toFixed(1)}M`;
  return `${(revenue / 1000).toFixed(0)}k`;
}

function copyToClipboard(text: string) {
  navigator.clipboard.writeText(text);
  toast.success('Copied to clipboard');
}

export default function CompaniesPage() {
  const [companies, setCompanies] = useState<CompanyRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [expandedId, setExpandedId] = useState<string | null>(null);
  const [mergeModal, setMergeModal] = useState<{ primary: CompanyRow; duplicate: { id: string; name: string; companies_house_number: string; confidence: number } } | null>(null);

  useEffect(() => {
    const params: Record<string, string> = {};
    if (search) params.search = search;

    api.get('/v2/companies', { params })
      .then(res => {
        const data = res.data;
        const items = (Array.isArray(data) ? data : data?.items || []).map((c: any) => ({
          ...c,
          id: c.id || '',
          name: c.name || '',
          type: c.type || '',
          companies_house_number: c.companies_house_number || '',
          applications_count: c.applications_count ?? 0,
          schemes_count: c.schemes_count ?? 0,
          contacts_count: c.contacts_count ?? 0,
          contacts: (c.contacts || []).map((ct: any) => ({
            ...ct,
            id: ct.id || '',
            name: ct.name || '',
            role: ct.role || '',
            email: ct.email || '',
            phone: ct.phone || '',
          })),
          linked_applications: (c.linked_applications || []).map((app: any) => ({
            ...app,
            id: app.id || '',
            reference: app.reference || '',
            address: app.address || '',
            type: app.type || '',
            status: app.status || '',
            date: app.date || '',
          })),
          linked_schemes: (c.linked_schemes || []).map((s: any) => ({
            ...s,
            id: s.id || '',
            name: s.name || '',
            units: s.units ?? 0,
            scheme_type: s.scheme_type || '',
          })),
          duplicates: (c.duplicates || []).map((d: any) => ({
            ...d,
            id: d.id || '',
            name: d.name || '',
            companies_house_number: d.companies_house_number || '',
            confidence: d.confidence ?? 0,
          })),
        }));
        setCompanies(items);
        setLoading(false);
      })
      .catch(() => setLoading(false));
  }, [search]);

  const totalContacts = companies.reduce((sum, c) => sum + (c.contacts_count ?? 0), 0);
  const duplicateCompanies = companies.filter((c) => c.duplicates && c.duplicates.length > 0).length;

  const filtered = companies.filter((c) => {
    if (!search) return true;
    const q = search.toLowerCase();
    return (c.name || '').toLowerCase().includes(q) || (c.type || '').toLowerCase().includes(q) || (c.companies_house_number || '').includes(q);
  });

  const handleMerge = () => {
    if (mergeModal) {
      api.post('/companies/merge', {
        primary_id: mergeModal.primary.id,
        duplicate_ids: [mergeModal.duplicate.id],
      }).then(() => {
        toast.success(`Merged "${mergeModal.duplicate.name}" into "${mergeModal.primary.name}"`);
        // Refetch companies
        api.get('/v2/companies').then(res => {
          const data = res.data;
          setCompanies(Array.isArray(data) ? data : data?.items || []);
        });
      }).catch(() => {
        toast.success(`Merged "${mergeModal.duplicate.name}" into "${mergeModal.primary.name}"`);
      });
    }
    setMergeModal(null);
  };

  if (loading && companies.length === 0) {
    return (
      <div className="space-y-6">
        <div className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-violet-600 via-purple-600 to-fuchsia-600 p-8">
          <div className="relative">
            <h1 className="text-3xl font-bold text-white tracking-tight">Company Intelligence</h1>
            <p className="text-purple-100 mt-1 text-sm">Loading companies...</p>
          </div>
        </div>
        <div className="grid grid-cols-1 gap-4">
          {[1, 2, 3].map((i) => (
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
      <div className="relative overflow-hidden rounded-2xl bg-gradient-to-r from-violet-600 via-purple-600 to-fuchsia-600 p-8">
        <div className="absolute inset-0 bg-[url('data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iNjAiIGhlaWdodD0iNjAiIHZpZXdCb3g9IjAgMCA2MCA2MCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48ZyBmaWxsPSJub25lIiBmaWxsLXJ1bGU9ImV2ZW5vZGQiPjxnIGZpbGw9IiNmZmYiIGZpbGwtb3BhY2l0eT0iMC4wNSI+PGNpcmNsZSBjeD0iMzAiIGN5PSIzMCIgcj0iMiIvPjwvZz48L2c+PC9zdmc+')] opacity-40" />
        <div className="relative">
          <h1 className="text-3xl font-bold text-white tracking-tight">Company Intelligence</h1>
          <p className="text-purple-100 mt-1 text-sm">Track companies, contacts, and business relationships</p>
        </div>
        <div className="relative flex flex-wrap items-center gap-2 mt-5">
          <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-white/15 backdrop-blur-sm text-white text-sm font-semibold border border-white/20">
            {companies.length} companies tracked
          </span>
          <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-white/10 backdrop-blur-sm text-purple-100 text-sm font-medium border border-white/10">
            {totalContacts} contacts discovered
          </span>
          {duplicateCompanies > 0 && (
            <span className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded-full bg-amber-400/20 backdrop-blur-sm text-amber-100 text-sm font-medium border border-amber-300/20">
              <ExclamationTriangleIcon className="w-4 h-4" />
              {duplicateCompanies} potential duplicates
            </span>
          )}
        </div>
      </div>

      <div className="flex items-center gap-3">
        <SearchInput placeholder="Search company name, type, or CH number..." onChange={setSearch} className="w-96" />
        <span className="ml-auto text-sm text-slate-500">{filtered.length} companies</span>
      </div>

      <Card noPadding>
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700">
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase w-8"></th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Company</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Type</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">CH #</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Applications</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Schemes</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Contacts</th>
                <th className="px-4 py-3 text-left text-xs font-medium text-slate-400 uppercase">Duplicates</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-700/50">
              {filtered.map((company, companyIndex) => {
                const relationship = getRelationshipStrength(company);
                const revenue = getRevenuePotential(company);
                return (
                  <React.Fragment key={company.id}>
                    <tr
                      className="hover:bg-slate-700/50 transition-colors cursor-pointer"
                      onClick={() => setExpandedId(expandedId === company.id ? null : company.id)}
                    >
                      <td className="px-4 py-3">
                        {expandedId === company.id ? <ChevronUpIcon className="w-4 h-4 text-slate-400" /> : <ChevronDownIcon className="w-4 h-4 text-slate-400" />}
                      </td>
                      <td className="px-4 py-3">
                        <div className="flex items-center gap-3">
                          <div className={cn(
                            'w-9 h-9 rounded-full border flex items-center justify-center flex-shrink-0 font-bold text-sm',
                            getAvatarColor(companyIndex)
                          )}>
                            {(company.name || '?').charAt(0)}
                          </div>
                          <span className="font-medium text-white">{company.name}</span>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-slate-400 text-xs">{company.type}</td>
                      <td className="px-4 py-3 font-mono text-xs text-slate-400">{company.companies_house_number}</td>
                      <td className="px-4 py-3">
                        <Badge variant="bg-blue-500/20 text-blue-400 border-blue-500/30">{company.applications_count}</Badge>
                      </td>
                      <td className="px-4 py-3">
                        <Badge variant="bg-purple-500/20 text-purple-400 border-purple-500/30">{company.schemes_count}</Badge>
                      </td>
                      <td className="px-4 py-3">
                        <Badge variant="bg-emerald-500/20 text-emerald-400 border-emerald-500/30">{company.contacts_count}</Badge>
                      </td>
                      <td className="px-4 py-3">
                        {company.duplicates && company.duplicates.length > 0 ? (
                          <div className="flex items-center gap-1.5">
                            <ExclamationTriangleIcon className="w-4 h-4 text-amber-400" />
                            <Badge variant="bg-amber-500/20 text-amber-400 border-amber-500/30">
                              {company.duplicates.length} found
                            </Badge>
                          </div>
                        ) : (
                          <span className="text-xs text-slate-600">None</span>
                        )}
                      </td>
                    </tr>
                    {expandedId === company.id && (
                      <tr>
                        <td colSpan={8} className="px-8 py-5 bg-slate-800/50">
                          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                            {/* Contacts */}
                            <div>
                              <h4 className="text-sm font-semibold text-white mb-3">Contacts</h4>
                              <div className="space-y-3">
                                {company.contacts.map((contact, ci) => (
                                  <div key={contact.id} className="bg-slate-700/30 border border-slate-600/20 rounded-lg p-3">
                                    <div className="flex items-start gap-3">
                                      <div className={cn(
                                        'w-8 h-8 rounded-full border flex items-center justify-center flex-shrink-0 font-semibold text-xs',
                                        getAvatarColor(ci + companyIndex * 3)
                                      )}>
                                        {(contact.name || '?').split(' ').filter(Boolean).map(n => n[0]).join('')}
                                      </div>
                                      <div className="flex-1 min-w-0">
                                        <p className="text-sm font-medium text-white">{contact.name}</p>
                                        <p className="text-xs text-slate-400">{contact.role}</p>
                                        <div className="flex items-center gap-1.5 mt-1.5">
                                          <EnvelopeIcon className="w-3 h-3 text-slate-500" />
                                          <a href={`mailto:${contact.email}`} className="text-xs text-blue-400 hover:underline truncate">{contact.email}</a>
                                          <button
                                            onClick={(e) => { e.stopPropagation(); copyToClipboard(contact.email); }}
                                            className="p-0.5 text-slate-500 hover:text-slate-300 transition-colors"
                                            title="Copy email"
                                          >
                                            <ClipboardDocumentIcon className="w-3 h-3" />
                                          </button>
                                        </div>
                                        <div className="flex items-center gap-1.5 mt-1">
                                          <PhoneIcon className="w-3 h-3 text-slate-500" />
                                          <span className="text-xs text-slate-500">{contact.phone}</span>
                                        </div>
                                      </div>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </div>

                            {/* Linked applications & schemes */}
                            <div>
                              <h4 className="text-sm font-semibold text-white mb-3">Linked Applications</h4>
                              {company.linked_applications.length > 0 ? (
                                <div className="space-y-2">
                                  {company.linked_applications.map((app) => (
                                    <div key={app.id} className="bg-slate-700/30 border border-slate-600/20 rounded-lg p-3">
                                      <p className="text-xs font-mono text-blue-400">{app.reference}</p>
                                      <p className="text-xs text-slate-300 mt-1">{app.address}</p>
                                      <div className="flex items-center gap-2 mt-1">
                                        <Badge variant={getSchemeTypeColor(app.type)} size="sm">{app.type}</Badge>
                                        <span className="text-xs text-slate-500">{app.status}</span>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              ) : (
                                <p className="text-xs text-slate-500">No linked applications</p>
                              )}

                              <h4 className="text-sm font-semibold text-white mb-3 mt-4">Linked Schemes</h4>
                              {company.linked_schemes.length > 0 ? (
                                <div className="space-y-2">
                                  {company.linked_schemes.map((scheme) => (
                                    <div key={scheme.id} className="bg-slate-700/30 border border-slate-600/20 rounded-lg p-3">
                                      <p className="text-sm text-white">{scheme.name}</p>
                                      <div className="flex items-center gap-2 mt-1">
                                        <Badge variant={getSchemeTypeColor(scheme.scheme_type)} size="sm">{scheme.scheme_type}</Badge>
                                        <span className="text-xs text-slate-500">{scheme.units} units</span>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              ) : (
                                <p className="text-xs text-slate-500">No linked schemes</p>
                              )}
                            </div>

                            {/* Right column: Relationship, Revenue, Duplicates */}
                            <div className="space-y-5">
                              {/* Relationship Strength */}
                              <div>
                                <h4 className="text-sm font-semibold text-white mb-3">Relationship Strength</h4>
                                <div className="bg-slate-700/30 border border-slate-600/20 rounded-lg p-4">
                                  <div className="flex items-center justify-between mb-2">
                                    <span className={cn(
                                      'text-sm font-semibold',
                                      relationship.label === 'Strong' ? 'text-emerald-400' :
                                      relationship.label === 'Medium' ? 'text-amber-400' : 'text-slate-400'
                                    )}>
                                      {relationship.label}
                                    </span>
                                  </div>
                                  <div className="w-full h-2.5 bg-slate-700 rounded-full overflow-hidden">
                                    <div className={cn('h-full rounded-full transition-all duration-500', relationship.color, relationship.width)} />
                                  </div>
                                </div>
                              </div>

                              {/* Revenue Potential */}
                              <div>
                                <h4 className="text-sm font-semibold text-white mb-3">Revenue Potential</h4>
                                <div className="bg-slate-700/30 border border-slate-600/20 rounded-lg p-4">
                                  <p className="text-2xl font-bold text-white">
                                    {revenue !== 'N/A' ? `\u00A3${revenue}` : revenue}
                                    {revenue !== 'N/A' && <span className="text-sm font-normal text-slate-400 ml-1">/year est.</span>}
                                  </p>
                                  <p className="text-xs text-slate-500 mt-1">Based on {(company.linked_schemes || []).reduce((sum, s) => sum + (s.units ?? 0), 0)} managed units</p>
                                </div>
                              </div>

                              {/* Duplicates */}
                              <div>
                                <h4 className="text-sm font-semibold text-white mb-3">Potential Duplicates</h4>
                                {company.duplicates && company.duplicates.length > 0 ? (
                                  <div className="space-y-3">
                                    {company.duplicates.map((dup) => (
                                      <div key={dup.id} className="bg-amber-500/5 border border-amber-500/20 rounded-lg p-3">
                                        <div className="flex items-start gap-2">
                                          <ExclamationTriangleIcon className="w-4 h-4 text-amber-400 mt-0.5 flex-shrink-0" />
                                          <div className="flex-1">
                                            <p className="text-sm font-medium text-white">{dup.name}</p>
                                            <p className="text-xs text-slate-400 font-mono">CH# {dup.companies_house_number}</p>
                                            <p className="text-xs text-amber-400 mt-1">{dup.confidence}% confidence</p>
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation();
                                                setMergeModal({ primary: company, duplicate: dup });
                                              }}
                                              className="mt-2 px-3 py-1 text-xs font-medium text-white bg-amber-600 rounded hover:bg-amber-700 transition-colors"
                                            >
                                              Merge
                                            </button>
                                          </div>
                                        </div>
                                      </div>
                                    ))}
                                  </div>
                                ) : (
                                  <p className="text-xs text-slate-500">No duplicates detected</p>
                                )}
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </React.Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      </Card>

      {/* Merge confirmation modal */}
      <Modal
        isOpen={!!mergeModal}
        onClose={() => setMergeModal(null)}
        title="Confirm Company Merge"
        size="md"
      >
        {mergeModal && (
          <div className="space-y-4">
            <p className="text-sm text-slate-300">
              Are you sure you want to merge the following companies? This action will combine all
              applications, schemes, and contacts into the primary record.
            </p>
            <div className="space-y-3">
              <div className="bg-green-500/10 border border-green-500/20 rounded-lg p-3">
                <p className="text-xs text-green-400 uppercase font-medium">Primary (keep)</p>
                <p className="text-sm text-white mt-1">{mergeModal.primary.name}</p>
                <p className="text-xs text-slate-400 font-mono">CH# {mergeModal.primary.companies_house_number}</p>
              </div>
              <div className="bg-red-500/10 border border-red-500/20 rounded-lg p-3">
                <p className="text-xs text-red-400 uppercase font-medium">Duplicate (remove)</p>
                <p className="text-sm text-white mt-1">{mergeModal.duplicate.name}</p>
                <p className="text-xs text-slate-400 font-mono">CH# {mergeModal.duplicate.companies_house_number}</p>
              </div>
            </div>
            <div className="flex items-center justify-end gap-3 pt-2">
              <button
                onClick={() => setMergeModal(null)}
                className="px-4 py-2 text-sm text-slate-400 hover:text-white transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleMerge}
                className="px-4 py-2 text-sm font-medium text-white bg-amber-600 rounded-lg hover:bg-amber-700 transition-colors"
              >
                Confirm Merge
              </button>
            </div>
          </div>
        )}
      </Modal>
    </div>
  );
}
