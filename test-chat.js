require('dotenv').config();
const { answerChatQuery } = require('./services/agent/chat-engine');

async function test() {
  try {
    const res = await answerChatQuery({
      query: "Did I finish the backend design?",
      options: { app: "Cursor" },
      onStep: (data) => console.log('Step:', data)
    });
    console.log('\n--- Final Response ---');
    console.log(res.content);
    console.log('\n--- Trace ---');
    console.log(JSON.stringify(res.thinking_trace, null, 2));
  } catch (e) {
    console.error(e);
  }
}

test();
