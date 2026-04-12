import JSONL from "jsonl-parse-stringify";

import { inngest } from "@/inngest/client";
import { StreamTranscriptItem } from "@/modules/meetings/types";
import { db } from "@/db";
import { inArray, eq } from "drizzle-orm";
import { agents, user, meetings } from "@/db/schema";
import { createAgent, openai } from "@inngest/agent-kit";

// ✅ AI Agent
const summarizer = createAgent({
  name: "summarizer",
  system: `
You are an expert summarizer. You write readable, concise, simple content.

### Overview
Provide a detailed summary.

### Notes
Break down key content into sections.
  `.trim(),
  model: openai({
    model: "gpt-4o",
    apiKey: process.env.OPENAI_API_KEY!,
  }),
});

// ✅ FIXED FUNCTION
export const meetingsProcessing = inngest.createFunction(
  {
    id: "meetings-processing",
    triggers: {
      event: "meetings/processing",
    },
  },
  async ({ event, step }: { event: any; step: any }) => {
    // ✅ fetch transcript safely
    const response = await step.fetch(event.data?.transcriptUrl);

  const rawTranscript = await step.run("parse-transcript", async () => {
  const text = await response.text();
  return JSONL.parse(text);
});

// ✅ THIS FIXES EVERYTHING
const transcript = rawTranscript as StreamTranscriptItem[];

    // ✅ add speaker names
    const transcriptWithSpeakers = await step.run("add-speakers", async () => {
      // ✅ FIX: explicitly typed
     const speakerIds: string[] = [
  ...new Set(
    transcript.map((item: StreamTranscriptItem) => item.speaker_id as string)
  ),
];

      const userSpeakers = await db
        .select()
        .from(user)
        .where(inArray(user.id, speakerIds));

      const agentSpeakers = await db
        .select()
        .from(agents)
        .where(inArray(agents.id, speakerIds));

      const speakers = [...userSpeakers, ...agentSpeakers];

      return transcript.map((item: StreamTranscriptItem) => {
        const speaker = speakers.find(
          (s) => s.id === item.speaker_id
        );

        return {
          ...item,
          user: {
            name: speaker?.name ?? "Unknown",
          },
        };
      });
    });

    // ✅ summarize
    const result = await summarizer.run(
      "Summarize this meeting:\n" +
        JSON.stringify(transcriptWithSpeakers)
    );

    // ✅ FIX: safe extraction
    const summary =
      result.output &&
      Array.isArray(result.output) &&
      result.output.length > 0 &&
      typeof result.output[0] === "object" &&
      "content" in result.output[0]
        ? (result.output[0] as any).content
        : JSON.stringify(result.output);

    // ✅ save to DB
    await step.run("save-summary", async () => {
      await db
        .update(meetings)
        .set({
          summary,
          status: "completed",
        })
        .where(eq(meetings.id, event.data?.meetingId));
    });

    return { success: true };
  }
);