import axios, { AxiosInstance } from 'axios';

const api: AxiosInstance = axios.create({
  baseURL: process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

api.interceptors.request.use(
  (config) => {
    const token = typeof window !== 'undefined' ? localStorage.getItem('auth_token') : null;
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

api.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      if (typeof window !== 'undefined') {
        localStorage.removeItem('auth_token');
      }
    }
    return Promise.reject(error);
  }
);

// Types
export interface PaginationParams {
  page?: number;
  limit?: number;
  search?: string;
  sort_by?: string;
  sort_order?: 'asc' | 'desc';
}

export interface ApplicationParams extends PaginationParams {
  council?: string;
  scheme_type?: string;
  status?: string;
  date_from?: string;
  date_to?: string;
}

export interface PipelineParams extends PaginationParams {
  stage?: string;
  scheme_type?: string;
  priority?: string;
  assigned_to?: string;
}

export interface AlertParams extends PaginationParams {
  type?: string;
  read?: boolean;
}

export interface Application {
  id: string;
  reference: string;
  address: string;
  postcode?: string;
  council: string;
  scheme_type: string;
  units: number;
  status: string;
  applicant: string;
  date: string;
  bd_score: number;
  description?: string;
  decision_date?: string;
  case_officer?: string;
}

export interface Company {
  id: string;
  name: string;
  type: string;
  companies_house_number: string;
  applications_count: number;
  schemes_count: number;
  contacts_count: number;
  contacts?: Contact[];
  linked_applications?: Application[];
  linked_schemes?: Scheme[];
  duplicates?: Company[];
}

export interface Contact {
  id: string;
  name: string;
  role: string;
  email: string;
  phone: string;
}

export interface PipelineOpportunity {
  id: string;
  company_name: string;
  company_id: string;
  scheme_type: string;
  stage: string;
  bd_score: number;
  priority: 'high' | 'medium' | 'low';
  assigned_to: string;
  council: string;
  units: number;
  estimated_value: number;
  contact_name: string;
  contact_email: string;
  notes: string;
  last_activity: string;
  created_at: string;
  activities?: Activity[];
}

export interface Activity {
  id: string;
  type: string;
  description: string;
  date: string;
  user: string;
}

export interface Scheme {
  id: string;
  name: string;
  operator: string;
  council: string;
  units: number;
  contract_end: string;
  performance: number;
  satisfaction: number;
  bd_score: number;
  priority: 'high' | 'medium' | 'low';
  scheme_type: string;
  address: string;
  postcode?: string;
  occupancy_rate?: number;
  revenue_per_unit?: number;
  score_breakdown?: ScoreBreakdown;
}

export interface ScoreBreakdown {
  contract_proximity: number;
  performance_gap: number;
  market_opportunity: number;
  relationship_strength: number;
  scheme_size: number;
}

export interface Alert {
  id: string;
  type: string;
  title: string;
  message: string;
  timestamp: string;
  read: boolean;
  link?: string;
}

export interface ScraperStatus {
  council_id: string;
  council_name: string;
  portal_type: string;
  last_run: string;
  success_rate: number;
  applications_found: number;
  status: 'healthy' | 'warning' | 'critical';
  error_message?: string;
  run_history?: ScraperRun[];
}

export interface ScraperRun {
  id: string;
  started_at: string;
  completed_at: string;
  duration_seconds: number;
  items_found: number;
  errors: number;
  status: 'success' | 'partial' | 'failed';
  error_details?: string;
}

export interface DashboardStats {
  total_applications: number;
  new_this_week: number;
  pipeline_opportunities: number;
  contracts_expiring_6m: number;
  total_applications_trend: number;
  new_this_week_trend: number;
  pipeline_trend: number;
  contracts_trend: number;
}

export interface TrendDataPoint {
  date: string;
  count: number;
}

export interface PipelineStats {
  total: number;
  by_stage: Record<string, number>;
  total_estimated_value: number;
  avg_bd_score: number;
}

// API Functions
export async function getApplications(params?: ApplicationParams) {
  const { data } = await api.get('/applications', { params });
  return data;
}

export async function getApplication(id: string) {
  const { data } = await api.get(`/applications/${id}`);
  return data;
}

export async function getApplicationStats() {
  const { data } = await api.get('/applications/stats');
  return data;
}

export async function getCompanies(params?: PaginationParams) {
  const { data } = await api.get('/companies', { params });
  return data;
}

export async function getCompany(id: string) {
  const { data } = await api.get(`/companies/${id}`);
  return data;
}

export async function mergeCompanies(primaryId: string, duplicateIds: string[]) {
  const { data } = await api.post('/companies/merge', { primary_id: primaryId, duplicate_ids: duplicateIds });
  return data;
}

export async function getPipelineOpportunities(params?: PipelineParams) {
  const { data } = await api.get('/pipeline', { params });
  return data;
}

export async function updateOpportunityStage(id: string, stage: string) {
  const { data } = await api.put(`/pipeline/${id}/stage`, { stage });
  return data;
}

export async function getPipelineStats() {
  const { data } = await api.get('/pipeline/stats');
  return data;
}

export async function getSchemes(params?: PaginationParams) {
  const { data } = await api.get('/schemes', { params });
  return data;
}

export async function getScheme(id: string) {
  const { data } = await api.get(`/schemes/${id}`);
  return data;
}

export async function getDashboardStats() {
  const { data } = await api.get('/dashboard/stats');
  return data;
}

export async function getTrendData(days: number = 30) {
  const { data } = await api.get('/dashboard/trends', { params: { days } });
  return data;
}

export async function getTopOpportunities() {
  const { data } = await api.get('/dashboard/top-opportunities');
  return data;
}

export async function getAlerts(params?: AlertParams) {
  const { data } = await api.get('/alerts', { params });
  return data;
}

export async function markAlertRead(id: string) {
  const { data } = await api.put(`/alerts/${id}/read`);
  return data;
}

export async function getScraperHealth() {
  const { data } = await api.get('/scrapers/health');
  return data;
}

export async function triggerScrape(councilId: string) {
  const { data } = await api.post(`/scrapers/${councilId}/trigger`);
  return data;
}

export async function getScraperHistory(councilId: string) {
  const { data } = await api.get(`/scrapers/${councilId}/history`);
  return data;
}

export default api;
