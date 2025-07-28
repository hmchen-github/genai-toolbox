/**
 * Handles the credential response from the Google Sign-In library.
 * @param {!CredentialResponse} response The credential response object from GIS.
 * @param {string} toolId The ID of the tool.
 * @param {string} authProfileName The name of the authentication profile.
 */
function handleCredentialResponse(response, toolId, authProfileName) { 
    console.log("handleCredentialResponse called with:", { response, toolId, authProfileName });
    const headersTextarea = document.getElementById(`headers-textarea-${toolId}`);
    if (!headersTextarea) {
        console.error('Headers textarea not found for toolId:', toolId);
        return;
    }

    const uniqueIdBase = `${toolId}-${authProfileName}`;
    const setupGisBtn = document.querySelector(`#google-auth-details-${uniqueIdBase} .setup-gis-btn`);
    const gisContainer = document.getElementById(`gisContainer-${uniqueIdBase}`);

    if (response.credential) {
        const idToken = response.credential;
        console.log("ID Token:", idToken);

        try {
            let currentHeaders = {};
            if (headersTextarea.value) {
                currentHeaders = JSON.parse(headersTextarea.value);
            }
            const headerKey = `${authProfileName}_token`; 
            currentHeaders[headerKey] = `${idToken}`;
            headersTextarea.value = JSON.stringify(currentHeaders, null, 2);
            // alert(`Header '${headerKey}' updated.`);

            if (gisContainer) gisContainer.style.display = 'none';
            if (setupGisBtn) setupGisBtn.style.display = '';

        } catch (e) {
            alert('Headers are not valid JSON. Please correct and try again.');
            console.error("Header JSON parse error:", e);
        }
    } else {
        console.error("Error: No credential in response", response);
        alert('Error: No ID Token received. Check console for details.');
        
        if (gisContainer) gisContainer.style.display = 'none';
        if (setupGisBtn) setupGisBtn.style.display = '';
    }
}

/**
 * Renders the Google Sign-In button using the GIS library.
 * @param {string} toolId The ID of the tool.
 * @param {string} clientId The Google OAuth Client ID.
 * @param {string} authProfileName The name of the authentication profile.
 */
function renderGoogleSignInButton(toolId, clientId, authProfileName) { 
    const uniqueIdBase = `${toolId}-${authProfileName}`;
    const gisContainerId = `gisContainer-${uniqueIdBase}`;
    const gisContainer = document.getElementById(gisContainerId);
    const setupGisBtn = document.querySelector(`#google-auth-details-${uniqueIdBase} .setup-gis-btn`);

    if (!gisContainer) {
        console.error('GIS container not found:', gisContainerId);
        return;
    }

    if (!clientId) {
        alert('Please enter an OAuth Client ID first.');
        return;
    }

    // Hide the setup button and show the container for the GIS button
    if (setupGisBtn) setupGisBtn.style.display = 'none';
    gisContainer.innerHTML = ''; // Clear previous button
    gisContainer.style.display = 'flex'; // Make it visible
    console.log(window.google, window.googleaccounts, window.google.accounts.id)
    if (window.google && window.google.accounts && window.google.accounts.id) {
        try {
            console.log("attempting handle response")
            const handleResponse = (response) => handleCredentialResponse(response, toolId, authProfileName);
            window.google.accounts.id.initialize({
                client_id: clientId,
                callback: handleResponse,
                auto_select: false
            });
            console.log("initialized account")
            window.google.accounts.id.renderButton(
                gisContainer,
                { theme: "outline", size: "large", text: "signin_with" }
            );
        } catch (error) {
            console.error("Error initializing Google Sign-In:", error);
            alert("Error initializing Google Sign-In. Check the Client ID and browser console.");
            gisContainer.innerHTML = '<p style="color: red;">Error loading Sign-In button.</p>';
            if (setupGisBtn) setupGisBtn.style.display = ''; 
        }
    } else {
        console.error("GIS library not fully loaded yet.");
        alert("Google Identity Services library not ready. Please try again in a moment.");
        gisContainer.innerHTML = '<p style="color: red;">GIS library not ready.</p>';
        if (setupGisBtn) setupGisBtn.style.display = ''; 
    }
}

// creates the Google Auth method dropdown
export function createGoogleAuthMethodItem(toolId, authProfileName) { 
    const item = document.createElement('div');
    item.className = 'auth-method-item';
    const uniqueIdBase = `${toolId}-${authProfileName}`;

    item.innerHTML = `
        <div class="auth-method-header">
            <span class="auth-method-label">Google ID Token (${authProfileName})</span>
            <button class="toggle-details-tab">Setup</button>
        </div>
        <div class="auth-method-details" id="google-auth-details-${uniqueIdBase}" style="display: none;">
            <div class="auth-controls">
                <div class="auth-input-row">
                    <label for="clientIdInput-${uniqueIdBase}">OAuth Client ID:</label>
                    <input type="text" id="clientIdInput-${uniqueIdBase}" placeholder="Enter Client ID" class="auth-input">
                </div>
                <div class="auth-method-actions">
                    <button class="btn btn--setup-gis">Add Token</button>
                    <div id="gisContainer-${uniqueIdBase}" class="auth-interactive-element gis-container" style="display: none;"></div>
                </div>
            </div>
        </div>
    `;

    const toggleBtn = item.querySelector('.toggle-details-tab');
    const detailsDiv = item.querySelector(`#google-auth-details-${uniqueIdBase}`);
    const setupGisBtn = item.querySelector('.btn--setup-gis');
    const clientIdInput = item.querySelector(`#clientIdInput-${uniqueIdBase}`);
    const gisContainer = item.querySelector(`#gisContainer-${uniqueIdBase}`);

    toggleBtn.addEventListener('click', () => {
        const isVisible = detailsDiv.style.display === 'flex'; 
        detailsDiv.style.display = isVisible ? 'none' : 'flex'; 
        toggleBtn.textContent = isVisible ? 'Setup' : 'Close';
        if (!isVisible) { 
            if (gisContainer) {
                gisContainer.innerHTML = '';
                gisContainer.style.display = 'none';
            }
            if (setupGisBtn) {
                setupGisBtn.style.display = ''; 
            }
        }
    });

    setupGisBtn.addEventListener('click', () => {
        const clientId = clientIdInput.value;
        if (!clientId) {
            alert('Please enter an OAuth Client ID first.');
            return;
        }
        renderGoogleSignInButton(toolId, clientId, authProfileName);
    });

    return item;
}
