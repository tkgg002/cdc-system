import axios from 'axios';

const AUTH_API = import.meta.env.VITE_AUTH_API_URL || 'http://localhost:8081';
const CMS_API = import.meta.env.VITE_CMS_API_URL || 'http://localhost:8083';
const WORKER_API = import.meta.env.VITE_WORKER_API_URL || 'http://localhost:8082';

// Auth API (no JWT needed)
export const authApi = axios.create({ baseURL: AUTH_API });

// CMS API (JWT required)
export const cmsApi = axios.create({ baseURL: CMS_API });

// Worker API (internal monitoring)
export const workerApi = axios.create({ baseURL: WORKER_API });

// Attach JWT token to CMS requests
cmsApi.interceptors.request.use((config) => {
  const token = localStorage.getItem('access_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Handle 401 — redirect to login
cmsApi.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('access_token');
      localStorage.removeItem('refresh_token');
      localStorage.removeItem('user');
      window.location.href = '/login';
    }
    return Promise.reject(error);
  }
);
