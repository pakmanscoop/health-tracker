const REPO_OWNER = "pakmanscoop";
const REPO_NAME = "health-data";
const FILE_PATH = "data.json";
const BRANCH = "main";
const API_BASE = "https://api.github.com";

const headers = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Content-Type": "application/json",
};

async function githubRequest(path, token, options = {}) {
  const url = `${API_BASE}${path}`;
  const res = await fetch(url, {
    ...options,
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/vnd.github.v3+json",
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
  const body = await res.json().catch(() => null);
  return { status: res.status, body };
}

exports.handler = async (event) => {
  if (event.httpMethod === "OPTIONS") {
    return { statusCode: 204, headers, body: "" };
  }

  if (event.httpMethod !== "POST") {
    return {
      statusCode: 405,
      headers,
      body: JSON.stringify({ success: false, error: "Method not allowed" }),
    };
  }

  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    return {
      statusCode: 503,
      headers,
      body: JSON.stringify({
        success: false,
        error:
          "GitHub token not configured. Add GITHUB_TOKEN to your Netlify environment variables.",
      }),
    };
  }

  try {
    const requestBody = JSON.parse(event.body);
    const { data, action } = requestBody;

    // Action: "fetch" to get remote data, "push" to update
    if (action === "fetch") {
      const getRes = await githubRequest(
        `/repos/${REPO_OWNER}/${REPO_NAME}/contents/${FILE_PATH}?ref=${BRANCH}`,
        token
      );

      if (getRes.status === 404) {
        return {
          statusCode: 404,
          headers,
          body: JSON.stringify({
            success: false,
            error: "Remote data.json not found",
          }),
        };
      }

      if (getRes.status !== 200) {
        return {
          statusCode: getRes.status,
          headers,
          body: JSON.stringify({
            success: false,
            error: `GitHub API error: ${getRes.body?.message || "Unknown error"}`,
          }),
        };
      }

      // Decode Base64 content
      const content = Buffer.from(getRes.body.content, "base64").toString(
        "utf-8"
      );
      const remoteData = JSON.parse(content);

      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          data: remoteData,
          sha: getRes.body.sha,
        }),
      };
    }

    // Action: "push" (default) — update data.json on GitHub
    if (!data) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({ success: false, error: "No data provided" }),
      };
    }

    // Step 1: Get current file SHA
    const getRes = await githubRequest(
      `/repos/${REPO_OWNER}/${REPO_NAME}/contents/${FILE_PATH}?ref=${BRANCH}`,
      token
    );

    let sha = null;
    if (getRes.status === 200) {
      sha = getRes.body.sha;
    } else if (getRes.status !== 404) {
      return {
        statusCode: getRes.status,
        headers,
        body: JSON.stringify({
          success: false,
          error: `GitHub API error: ${getRes.body?.message || "Unknown error"}`,
        }),
      };
    }

    // Step 2: Update (or create) the file
    const now = new Date();
    const dateStr = now.toISOString().split("T")[0];
    const timeStr = now.toTimeString().split(" ")[0].substring(0, 5);
    const commitMessage = `Sync health data \u2013 ${dateStr} ${timeStr}`;

    const content = Buffer.from(
      JSON.stringify(data, null, 2) + "\n",
      "utf-8"
    ).toString("base64");

    const putBody = {
      message: commitMessage,
      content: content,
      branch: BRANCH,
    };
    if (sha) {
      putBody.sha = sha;
    }

    const putRes = await githubRequest(
      `/repos/${REPO_OWNER}/${REPO_NAME}/contents/${FILE_PATH}`,
      token,
      {
        method: "PUT",
        body: JSON.stringify(putBody),
      }
    );

    if (putRes.status === 200 || putRes.status === 201) {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          success: true,
          sha: putRes.body.content?.sha,
          committed_at: now.toISOString(),
        }),
      };
    }

    if (putRes.status === 409) {
      return {
        statusCode: 409,
        headers,
        body: JSON.stringify({
          success: false,
          error: "conflict",
          message:
            "Data on GitHub has changed since your last sync. Choose to keep your local data or fetch the remote version.",
        }),
      };
    }

    if (putRes.status === 401 || putRes.status === 403) {
      return {
        statusCode: 401,
        headers,
        body: JSON.stringify({
          success: false,
          error: `GitHub authentication failed. Check your GITHUB_TOKEN has repo scope.`,
        }),
      };
    }

    return {
      statusCode: putRes.status || 500,
      headers,
      body: JSON.stringify({
        success: false,
        error: `GitHub API error: ${putRes.body?.message || "Unknown error"}`,
      }),
    };
  } catch (err) {
    console.error("Sync to GitHub error:", err);
    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        success: false,
        error: "Failed to sync. Check your connection and try again.",
      }),
    };
  }
};
