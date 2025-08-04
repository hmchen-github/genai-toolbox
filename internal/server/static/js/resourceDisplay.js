// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const MOCK_SOURCE_MANIFEST = {
    "sources": {
        "my-alloydb-pg-source": {
            kind: "alloydb-postgres",
            project: "my-project-id",
            region: "us-central1",
            cluster: "my-cluster",
            instance: "my-instance",
            database: "my_db",
            ipType: "public"
        },
        "my-bigtable-source": {
            kind: "bigtable",
            project: "my-project-id",
            instance: "test-instance"
        },
    }
};

async function getMockSourceListData() {
    console.warn(`[MOCK] Returning mock data for all sources`);
    return MOCK_SOURCE_MANIFEST;
}

async function getMockSourceDetailsData(sourceName) {
    console.warn(`[MOCK] Returning mock data for source details: ${sourceName}`);
    const SOURCE_DATA = MOCK_SOURCE_MANIFEST.sources[sourceName];
    if (SOURCE_DATA) {
        return { "sources": { [sourceName]: SOURCE_DATA } };
    } else {
        throw new Error(`Mock Source "${sourceName}" not found`);
    }
}

const MOCK_AUTH_SERVICE_MANIFEST = {
    "authservices": {
        "my-auth-service-1": {
            kind: "google oauth2",
        },
        "another-auth": {
            kind: "other",
        }
    }
};

async function getMockAuthServiceListData() {
    console.warn(`[MOCK] Returning mock data for all authservices`);
    return MOCK_AUTH_SERVICE_MANIFEST;
}

async function getMockAuthServiceDetailsData(authServiceName) {
    console.warn(`[MOCK] Returning mock data for auth service details: ${authServiceName}`);
    const SERVICE_DATA = MOCK_AUTH_SERVICE_MANIFEST.authservices[authServiceName];
    if (SERVICE_DATA) {
        return { "authservices": { [authServiceName]: SERVICE_DATA } };
    } else {
        throw new Error(`Mock Auth Service "${authServiceName}" not found`);
    }
}

const itemConfigs = {
    sources: {
        getListData: getMockSourceListData, // replace with real API call to GET all sources
        getDetailsData: getMockSourceDetailsData, // replace with real API call to GET specific source
        listKey: 'sources',
        nameAttribute: 'data-itemname',
        displayName: 'Source',
        singularName: 'source',
    },
    authservices: {
        getListData: getMockAuthServiceListData, // replace with real API call to GET all auth services
        getDetailsData: getMockAuthServiceDetailsData, //replace with real API call to get specific auth service
        listKey: 'authservices',
        nameAttribute: 'data-itemname',
        displayName: 'Auth Service',
        singularName: 'authservice',
    }
};

/**
 * Renders the details of an item.
 * @param {string} itemName - The name of the item.
 * @param {!Object<string, *>} itemDetails - The object containing the item's attributes.
 * @param {!HTMLElement} displayArea - The HTML element to render the details into.
 * @param {string} displayName - The display name of the item type.
 */
function displayItemDetails(itemName, itemDetails, displayArea, displayName) {
    displayArea.innerHTML = '';

    const title = document.createElement('h3');
    title.textContent = `${displayName}: ${itemName}`;
    displayArea.appendChild(title);

    const entries = Object.entries(itemDetails);
    if (entries.length === 0) {
        const para = document.createElement('p');
        para.textContent = 'No details available.';
        displayArea.appendChild(para);
        return;
    }

    const ul = document.createElement('ul');
    ul.classList.add('resource-details-list');

    for (const [key, value] of entries) {
        const li = document.createElement('li');
        li.classList.add('resource-detail-item');

        const keyDiv = document.createElement('div');
        keyDiv.classList.add('key');
        keyDiv.textContent = key;
        li.appendChild(keyDiv);

        const valueDiv = document.createElement('div');
        valueDiv.classList.add('value');
        valueDiv.textContent = String(value);
        li.appendChild(valueDiv);

        ul.appendChild(li);
    }
    displayArea.appendChild(ul);
}

/**
 * Fetches and displays details for a specific item.
 * @param {string} itemType - 'sources' or 'authservices'.
 * @param {string} itemName - The name of the item.
 * @param {!HTMLElement} displayArea - The element to render details into.
 * @param {!Object} config - The configuration object for the item type.
 */
async function fetchItemDetails(itemType, itemName, displayArea, config) {
    displayArea.innerHTML = `<p>Loading details for ${itemName}...</p>`;
    try {
        const apiResponse = await config.getDetailsData(itemName);
        const itemDetails = apiResponse && apiResponse[config.listKey] ? apiResponse[config.listKey][itemName] : null;

        if (!itemDetails) {
            throw new Error(`${config.displayName} "${itemName}" data not found in API response.`);
        }

        displayItemDetails(itemName, itemDetails, displayArea, config.displayName);
        console.log(`${config.displayName} details:`, itemDetails);

    } catch (error) {
        console.error(`Failed to load details for ${config.singularName} "${itemName}":`, error);
        displayArea.innerHTML = `<p class="error">Failed to load details for ${itemName}: ${error.message}</p>`;
    }
}

/**
 * Handles the click event on an item button.
 * @param {!Event} event - The click event object.
 * @param {string} itemType - 'sources' or 'authservices'.
 * @param {!HTMLElement} listContainer - The parent element containing the item buttons.
 * @param {!HTMLElement} displayArea - The HTML element where item details will be shown.
 * @param {!Object} config - The configuration object for the item type.
 */
function handleItemClick(event, itemType, listContainer, displayArea, config) {
    const itemName = event.target.getAttribute(config.nameAttribute);
    if (itemName) {
        const currentActive = listContainer.querySelector('.second-nav-choice.active');
        if (currentActive) currentActive.classList.remove('active');
        event.target.classList.add('active');
        fetchItemDetails(itemType, itemName, displayArea, config);
    }
}

/**
 * Renders the list of items as buttons.
 * @param {string} itemType - 'sources' or 'authservices'.
 * @param {?Object} apiResponse - The API response object.
 * @param {!HTMLElement} listContainer - The element to render the list into.
 * @param {!HTMLElement} displayArea - The element for displaying item details.
 * @param {!Object} config - The configuration object for the item type.
 */
function renderList(itemType, apiResponse, listContainer, displayArea, config) {
    listContainer.innerHTML = '';

    const itemsObject = apiResponse ? apiResponse[config.listKey] : null;

    if (!itemsObject || typeof itemsObject !== 'object' || itemsObject === null) {
        console.error(`Error: Expected an object with a "${config.listKey}" property, but received:`, apiResponse);
        listContainer.textContent = `Error: Invalid response format from ${config.displayName} API.`;
        return;
    }

    const itemNames = Object.keys(itemsObject);

    if (itemNames.length === 0) {
        listContainer.textContent = `No ${config.displayName}s found.`;
        return;
    }

    const ul = document.createElement('ul');
    itemNames.forEach(itemName => {
        const li = document.createElement('li');
        const button = document.createElement('button');
        button.textContent = itemName;
        button.setAttribute(config.nameAttribute, itemName);
        button.classList.add('second-nav-choice');
        button.addEventListener('click', (event) => handleItemClick(event, itemType, listContainer, displayArea, config));
        li.appendChild(button);
        ul.appendChild(li);
    });
    listContainer.appendChild(ul);
}

/**
 * Fetches and renders a list of items (sources or authservices).
 * @param {string} itemType - 'sources' or 'authservices'.
 * @param {!HTMLElement} listContainer - Element to render the list into.
 * @param {!HTMLElement} displayArea - Element to display item details.
 * @returns {!Promise<void>}
 */
async function loadItems(itemType, listContainer, displayArea) {
    const config = itemConfigs[itemType];
    if (!config) {
        console.error(`Unknown item type: ${itemType}`);
        if (listContainer) {
            listContainer.innerHTML = `<p class="error">Configuration error.</p>`;
        }
        return;
    }

    if (!listContainer || !displayArea) {
        console.error("List container or display area not provided to loadItems");
        return;
    }

    listContainer.innerHTML = `<p>Fetching ${config.displayName}s...</p>`;
    try {
        const apiResponse = await config.getListData();
        renderList(itemType, apiResponse, listContainer, displayArea, config);
    } catch (error) {
        console.error(`Failed to load ${config.displayName}s:`, error);
        listContainer.innerHTML = `<p class="error">Failed to load ${config.displayName}s: <pre><code>${error}</code></pre></p>`;
    }
}

export { loadItems };
