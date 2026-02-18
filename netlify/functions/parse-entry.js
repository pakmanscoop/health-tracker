const Anthropic = require("@anthropic-ai/sdk");

const MEAL_SYSTEM_PROMPT = `You are a nutrition analysis assistant. Given a natural language description of a meal, extract structured nutritional data. You must respond with ONLY valid JSON, no markdown, no code fences, no other text.

Use these fields:
- description: Brief clean description of the meal
- meal_type: One of "Breakfast", "Lunch", "Dinner", "Snack" (use the hint if provided, otherwise infer from context or time)
- time: 24-hour format HH:MM (use the hint if provided, otherwise infer: breakfast ~08:00, lunch ~12:00, dinner ~18:30, snack ~15:00)
- net_carbs: integer grams (total carbs minus fiber)
- protein: integer grams
- fat: integer grams (total fat)
- saturated_fat: integer grams
- fiber: integer grams
- calories: integer kcal
- tier: "Green" if net_carbs <= 20, "Yellow" if 21-40, "Red" if > 40
- notes: One brief observation about the meal

Estimate reasonable portions if not specified. Be conservative with estimates. Round all numbers to integers.`;

const EXERCISE_SYSTEM_PROMPT = `You are a fitness tracking assistant. Given a natural language description of exercise, extract structured workout data. You must respond with ONLY valid JSON, no markdown, no code fences, no other text.

Use these fields:
- exercise_type: One of "Cardio", "Strength", "Cardio+Strength", "Cardio+HIIT", "Flexibility", "Sports"
- description: Brief clean description of the workout
- time: 24-hour format HH:MM (use the hint if provided, otherwise default to 08:00)
- duration_min: integer minutes (estimate if not specified)
- distance_km: number with one decimal (0.0 if not applicable; convert miles to km: 1 mile = 1.6 km)
- sets_reps: Formatted as "Exercise: SetsxReps | Exercise: SetsxReps" (empty string if pure cardio with no sets)
- intensity: One of "Low", "Moderate", "High"
- notes: One brief observation

Estimate reasonable values if not explicitly specified.`;

const headers = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Headers": "Content-Type",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Content-Type": "application/json",
};

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

  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    return {
      statusCode: 503,
      headers,
      body: JSON.stringify({
        success: false,
        error:
          "API key not configured. Add ANTHROPIC_API_KEY to your Netlify environment variables.",
      }),
    };
  }

  try {
    const { type, text, mealType, time } = JSON.parse(event.body);

    if (!text || !text.trim()) {
      return {
        statusCode: 400,
        headers,
        body: JSON.stringify({
          success: false,
          error: "No input text provided",
        }),
      };
    }

    const client = new Anthropic({ apiKey });

    const systemPrompt =
      type === "exercise" ? EXERCISE_SYSTEM_PROMPT : MEAL_SYSTEM_PROMPT;

    const userMessage =
      type === "exercise"
        ? `Time hint: ${time || "not specified"}\n\nUser input: ${text}`
        : `Meal type hint: ${mealType || "not specified"}\nTime hint: ${time || "not specified"}\n\nUser input: ${text}`;

    const response = await client.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 512,
      system: systemPrompt,
      messages: [{ role: "user", content: userMessage }],
    });

    const content = response.content[0].text;

    // Parse JSON — handle potential markdown code fences
    let parsed;
    const jsonMatch = content.match(/```(?:json)?\s*([\s\S]*?)```/);
    parsed = JSON.parse(jsonMatch ? jsonMatch[1].trim() : content.trim());

    return {
      statusCode: 200,
      headers,
      body: JSON.stringify({ success: true, entry: { type, ...parsed } }),
    };
  } catch (err) {
    console.error("Parse entry error:", err);

    if (err.status === 401) {
      return {
        statusCode: 503,
        headers,
        body: JSON.stringify({
          success: false,
          error: "Invalid API key. Check your ANTHROPIC_API_KEY.",
        }),
      };
    }

    return {
      statusCode: 500,
      headers,
      body: JSON.stringify({
        success: false,
        error:
          "Failed to parse entry. Try again with a clearer description.",
      }),
    };
  }
};
