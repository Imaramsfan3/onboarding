import React, { useState } from 'react';
import { submitAddress, lookupPostcode } from '../services/api';

function AddressDetails({ applicationId, data, onSave, onNext, onBack }) {
  const [form, setForm] = useState({
    address_line_1: data.address_line_1 || '',
    address_line_2: data.address_line_2 || '',
    city: data.city || '',
    county: data.county || '',
    postcode: data.postcode || '',
    country: data.country || 'United Kingdom',
  });
  const [lookupPostcode_, setLookupPostcode] = useState(data.postcode || '');
  const [addresses, setAddresses] = useState([]);
  const [lookupLoading, setLookupLoading] = useState(false);
  const [lookupError, setLookupError] = useState('');
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState('');

  const handleChange = (e) => {
    const { name, value } = e.target;
    setForm((prev) => ({ ...prev, [name]: value }));
  };

  const handlePostcodeLookup = async () => {
    if (!lookupPostcode_.trim()) return;

    setLookupError('');
    setLookupLoading(true);
    setAddresses([]);

    try {
      const result = await lookupPostcode(lookupPostcode_);
      const addressList = result.addresses || result || [];
      if (Array.isArray(addressList) && addressList.length > 0) {
        setAddresses(addressList);
      } else {
        setLookupError('No addresses found for this postcode.');
      }
    } catch (err) {
      const message =
        err.response?.data?.detail ||
        err.response?.data?.message ||
        'Failed to look up postcode. Please try again.';
      setLookupError(message);
    } finally {
      setLookupLoading(false);
    }
  };

  const handleSelectAddress = (address) => {
    setForm((prev) => ({
      ...prev,
      address_line_1: address.address_line_1 || address.line_1 || '',
      address_line_2: address.address_line_2 || address.line_2 || '',
      city: address.city || address.town_or_city || '',
      county: address.county || '',
      postcode: address.postcode || lookupPostcode_,
      country: address.country || 'United Kingdom',
    }));
    setAddresses([]);
  };

  const formatAddressDisplay = (address) => {
    const parts = [
      address.address_line_1 || address.line_1,
      address.address_line_2 || address.line_2,
      address.city || address.town_or_city,
      address.county,
      address.postcode,
    ].filter(Boolean);
    return parts.join(', ');
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setSubmitting(true);

    try {
      await submitAddress({
        application_id: applicationId,
        address_line_1: form.address_line_1,
        address_line_2: form.address_line_2,
        city: form.city,
        county: form.county,
        postcode: form.postcode,
        country: form.country,
      });
      onSave(form);
      onNext();
    } catch (err) {
      const message =
        err.response?.data?.detail ||
        err.response?.data?.message ||
        'Failed to submit address. Please try again.';
      setError(message);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="card">
      <h2 className="card-title">Address</h2>
      <p className="card-subtitle">
        Enter your residential address or look it up by postcode.
      </p>

      {error && <div className="error-message">{error}</div>}

      {/* Postcode Lookup */}
      <div className="postcode-row" style={{ marginBottom: '1.25rem' }}>
        <div className="form-group">
          <label className="form-label" htmlFor="postcode-lookup">
            Postcode Lookup
          </label>
          <input
            id="postcode-lookup"
            type="text"
            className="form-input"
            placeholder="e.g. SW1A 1AA"
            value={lookupPostcode_}
            onChange={(e) => setLookupPostcode(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                e.preventDefault();
                handlePostcodeLookup();
              }
            }}
          />
        </div>
        <button
          type="button"
          className="btn btn-lookup"
          onClick={handlePostcodeLookup}
          disabled={lookupLoading || !lookupPostcode_.trim()}
        >
          {lookupLoading ? 'Searching...' : 'Find Address'}
        </button>
      </div>

      {lookupError && <div className="error-message">{lookupError}</div>}

      {addresses.length > 0 && (
        <div className="dropdown-container">
          <span className="dropdown-label">
            Select an address ({addresses.length} found):
          </span>
          <div className="results-list">
            {addresses.map((addr, idx) => (
              <div
                key={idx}
                className="results-item"
                onClick={() => handleSelectAddress(addr)}
              >
                {formatAddressDisplay(addr)}
              </div>
            ))}
          </div>
        </div>
      )}

      <form onSubmit={handleSubmit}>
        <div className="form-group">
          <label className="form-label" htmlFor="address_line_1">
            Address Line 1 <span className="required">*</span>
          </label>
          <input
            id="address_line_1"
            name="address_line_1"
            type="text"
            className="form-input"
            placeholder="House number and street"
            value={form.address_line_1}
            onChange={handleChange}
            required
          />
        </div>

        <div className="form-group">
          <label className="form-label" htmlFor="address_line_2">
            Address Line 2
          </label>
          <input
            id="address_line_2"
            name="address_line_2"
            type="text"
            className="form-input"
            placeholder="Apartment, suite, etc. (optional)"
            value={form.address_line_2}
            onChange={handleChange}
          />
        </div>

        <div className="form-row">
          <div className="form-group">
            <label className="form-label" htmlFor="city">
              City <span className="required">*</span>
            </label>
            <input
              id="city"
              name="city"
              type="text"
              className="form-input"
              placeholder="City"
              value={form.city}
              onChange={handleChange}
              required
            />
          </div>

          <div className="form-group">
            <label className="form-label" htmlFor="county">
              County
            </label>
            <input
              id="county"
              name="county"
              type="text"
              className="form-input"
              placeholder="County"
              value={form.county}
              onChange={handleChange}
            />
          </div>
        </div>

        <div className="form-row">
          <div className="form-group">
            <label className="form-label" htmlFor="postcode">
              Postcode <span className="required">*</span>
            </label>
            <input
              id="postcode"
              name="postcode"
              type="text"
              className="form-input"
              placeholder="Postcode"
              value={form.postcode}
              onChange={handleChange}
              required
            />
          </div>

          <div className="form-group">
            <label className="form-label" htmlFor="country">
              Country <span className="required">*</span>
            </label>
            <input
              id="country"
              name="country"
              type="text"
              className="form-input"
              placeholder="Country"
              value={form.country}
              onChange={handleChange}
              required
            />
          </div>
        </div>

        <div className="btn-group">
          <button
            type="button"
            className="btn btn-secondary"
            onClick={onBack}
            disabled={submitting}
          >
            Back
          </button>
          <button
            type="submit"
            className="btn btn-primary"
            disabled={submitting}
          >
            {submitting ? 'Submitting...' : 'Next'}
          </button>
        </div>
      </form>
    </div>
  );
}

export default AddressDetails;
