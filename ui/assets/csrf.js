/* global window */
function installCsrfInterceptor(axios, location) {
  axios.interceptors.request.use(async (request) => {
    const url = new URL(request.url, request.baseURL || location.href);
    const method = (request.method || 'get').toUpperCase();
    const path = url.pathname.toLowerCase().replace(/\/+$/, '');
    const changesState = !['GET', 'HEAD', 'OPTIONS'].includes(method) || ['/logout', '/isloggedin'].includes(path);
    if (url.origin !== location.origin || !changesState || path === '/verifylogin') return request;
    // Fetch per write to follow session changes in other tabs. Do not retry writes.
    const response = await axios.get(`${location.origin}/csrf-token`);
    const token = response.data.csrfToken;
    if (typeof token !== 'string' || !/^[a-f0-9]{64}$/.test(token)) throw new Error('Unable to authorize this request. Reload and sign in again.');
    Object.assign(request, { headers: { ...request.headers, 'X-CSRF-Token': token } });
    return request;
  });
}

if (typeof module !== 'undefined' && module.exports) module.exports = installCsrfInterceptor;
else installCsrfInterceptor(window.axios, window.location);
