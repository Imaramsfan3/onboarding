import axios from 'axios';

const BFF_URL = process.env.REACT_APP_BFF_URL || 'http://localhost:8000';

const api = axios.create({
  baseURL: BFF_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

export const submitPersonalDetails = async (data) => {
  const response = await api.post('/api/personal-details', data);
  return response.data;
};

export const submitAddress = async (data) => {
  const response = await api.post('/api/address', data);
  return response.data;
};

export const submitBusinessDetails = async (data) => {
  const response = await api.post('/api/business-details', data);
  return response.data;
};

export const lookupPostcode = async (postcode) => {
  const encoded = encodeURIComponent(postcode.trim());
  const response = await api.get(`/api/postcode-lookup/${encoded}`);
  return response.data;
};

export const searchCompany = async (query) => {
  const response = await api.get('/api/company-search', {
    params: { q: query },
  });
  return response.data;
};

export const getApplicationStatus = async (applicationId) => {
  const response = await api.get(`/api/application-status/${applicationId}`);
  return response.data;
};

export default api;
