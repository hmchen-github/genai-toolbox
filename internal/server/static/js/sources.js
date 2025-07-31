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

document.addEventListener('DOMContentLoaded', () => {
    const sourceInfoArea = document.getElementById('source-info-area');
    const secondaryPanelContent = document.getElementById('secondary-panel-content');

    if (!secondaryPanelContent || !sourceInfoArea) {
        console.error('Required DOM elements not found.');
        return;
    }

    loadSources(secondaryPanelContent, sourceInfoArea)
});

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
        "my-couchbase-instance": {
            kind: "couchbase",
            connectionString: "couchebase://localhost:8091",
            bucket: "travel-sample",
            scope: "inventory"
        }
    }
};

/**
 * Returns mock data for the source list.
 * @returns {!Promise<Object>} Mocked API response data.
 */
async function getMockSourceListData() {
    console.warn(`[MOCK] Returning mock data for all sources`);
    return MOCK_SOURCE_MANIFEST;
}

/**
 * Returns mock data for source details in the specified JSON format.
 * @param {string} sourceName The name of the source.
 * @returns {!Promise<Object>} Mocked API response data.
 */
async function getMockSourceDetailsData(sourceName) {
    console.warn(`[MOCK] Returning mock data for source details: ${sourceName}`);
    const SOURCE_DATA = MOCK_SOURCE_MANIFEST.sources[sourceName]; 
    if (SOURCE_DATA) {
        return {
            "sources": {
                [sourceName]: SOURCE_DATA
            }
        };
    } else {
        throw new Error(`Mock Source "${sourceName}" not found`);
    }
}

/**
 * Fetches a source list from the /api/sources endpoint and initiates creating the source list.
 * @param {!HTMLElement} secondNavContent The HTML element where the source list will be rendered.
 * @param {!HTMLElement} displayArea The HTML element where the details of a selected source will be displayed.
 * @returns {!Promise<void>} A promise that resolves when the sources are loaded and rendered, or rejects on error.
 */
export async function loadSources(secondNavContent, displayArea) {
    secondNavContent.innerHTML = '<p>Fetching sources...</p>';
    try {
        // MOCK VERSION OF API CALL - GET ALL SOURCES
        const apiResponse = await getMockSourceListData();

        // REAL API CALL - GET ALL SOURCES
        // const response = await fetch(`/api/sources/`);
        // if (!response.ok) {
        //     throw new Error(`HTTP error! status: ${response.status} ${response.statusText}`);
        // }
        // const apiResponse = await response.json();

        renderSourceList(apiResponse, secondNavContent, displayArea);
    } catch (error) {
        console.error('Failed to load sources:', error);
        secondNavContent.innerHTML = `<p class="error">Failed to load sources: <pre><code>${error}</code></pre></p>`;
    }
}

/**
 * Renders the list of sources as buttons within the provided HTML element.
 * @param {?{sources: ?Object<string,*>} } apiResponse The API response object containing the sources.
 * @param {!HTMLElement} secondNavContent The HTML element to render the source list into.
 * @param {!HTMLElement} displayArea The HTML element for displaying source details.
 */
function renderSourceList(apiResponse, secondNavContent, displayArea) {
    secondNavContent.innerHTML = '';
    console.log(apiResponse)
    if (!apiResponse || typeof apiResponse.sources !== 'object' || apiResponse.sources === null) {
        console.error('Error: Expected an object with a "sources" property, but received:', apiResponse);
        secondNavContent.textContent = 'Error: Invalid response format from sources API.';
        return;
    }

    const sourcesObject = apiResponse.sources;
    const sourceNames = Object.keys(sourcesObject);

    if (sourceNames.length === 0) {
        secondNavContent.textContent = 'No sources found.';
        return;
    }

    const ul = document.createElement('ul');
    sourceNames.forEach(sourceName => {
        const li = document.createElement('li');
        const button = document.createElement('button');
        button.textContent = sourceName;
        button.dataset.sourcename = sourceName;
        button.classList.add('second-nav-choice');
        button.addEventListener('click', (event) => handleSourceClick(event, secondNavContent, displayArea));
        li.appendChild(button);
        ul.appendChild(li);
    });
    secondNavContent.appendChild(ul);
}

/**
 * Handles the click event on a source button.
 * @param {!Event} event The click event object.
 * @param {!HTMLElement} secondNavContent The parent element containing the source buttons.
 * @param {!HTMLElement} displayArea The HTML element where source details will be shown.
 */
function handleSourceClick(event, secondNavContent, displayArea) {
    const sourceName = event.target.dataset.sourcename;
    if (sourceName) {
        const currentActive = secondNavContent.querySelector('.second-nav-choice.active'); 
        if (currentActive) currentActive.classList.remove('active');
        event.target.classList.add('active');
        fetchSourceDetails(sourceName, displayArea);
    }
}

/**
 * Fetches and displays details for a specific source.
 * @param {string} sourceName The name of the source.
 * @param {!HTMLElement} displayArea The element to render details into.
 */
async function fetchSourceDetails(sourceName, displayArea) {

    displayArea.innerHTML = `<p>Loading details for ${sourceName}...</p>`;
    try {
        // MOCK VERSION OF API CALL - GET SPECIFIC SOURCES BY NAME
        const apiResponse = await getMockSourceDetailsData(sourceName);

        // REAL API CALL - GET SPECIFIC SOURCES BY NAME
        // const response = await fetch(`/api/source/${encodeURIComponent(sourceName)}`, { signal });
        // if (!response.ok) {
        //     throw new Error(`HTTP error! status: ${response.status} ${response.statusText}`);
        // }
        // const apiResponse = await response.json();

        // Adjusted check for the new structure
        if (!apiResponse.sources || !apiResponse.sources[sourceName]) {
            throw new Error(`Source "${sourceName}" data not found in API response.`);
        }

        const sourceDetails = apiResponse.sources[sourceName]; 
        displaySourceDetails(sourceName, sourceDetails, displayArea);
        console.log("Source details:", sourceDetails);

    } catch (error) {
        if (error.name === 'AbortError') {
            console.debug("Source detail fetch aborted.");
        } else {
            console.error(`Failed to load details for source "${sourceName}":`, error);
            displayArea.innerHTML = `<p class="error">Failed to load details for ${sourceName}: ${error.message}</p>`;
        }
    }
}

/**
 * Renders the details of a source manifest in a user-friendly format.
 * @param {string} sourceName The name of the source.
 * @param {!Object<string, *>} sourceDetails The object containing the source's attributes.
 * @param {!HTMLElement} displayArea The HTML element to render the details into.
 */
function displaySourceDetails(sourceName, sourceDetails, displayArea) {
    displayArea.innerHTML = ''; 

    const title = document.createElement('h3');
    const entries = Object.entries(sourceDetails);

    title.textContent = `${sourceName}:`;
    displayArea.appendChild(title);

    if (entries.length === 0) {
        const para = document.createElement('p');
        para.textContent = 'No details available.';
        displayArea.appendChild(para);
        return;
    }

    // SINGLE LIST

    // const ul = document.createElement('ul');
    // ul.classList.add('source-details-list');

    // for (const [key, value] of entries) {
    //     const li = document.createElement('li');
    //     const keyElement = document.createElement('strong');
    //     const valueElement = document.createElement('span');

    //     li.classList.add('source-detail-item');
    //     keyElement.textContent = `${key}: `;
    //     keyElement.classList.add('key')
    //     li.appendChild(keyElement);
    //     valueElement.textContent = String(value);
    //     valueElement.classList.add('value')
    //     li.appendChild(valueElement);
    //     ul.appendChild(li);
    // }
    // displayArea.appendChild(ul);

    // DOUBLE LIST
    const ul = document.createElement('ul');
    ul.classList.add('source-details-list');

    for (const [key, value] of entries) {
        const li = document.createElement('li');
        li.classList.add('source-detail-item');

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
