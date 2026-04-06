import { Client } from "../src";

const c = await Client.dial(":6379");

try {
  // ── String ──
  await c.set("apple", "phone");
  const apple = await c.get("apple");
  console.log(`KEY=apple, VALUE=${apple}`);

  // ── Hash ──
  const userid = "user:123";
  await c.hSet(userid, "name", "cozzytree");
  const name = await c.hGet(userid, "name");
  console.log(`HGET=${userid}, VALUE=${name}`);

  // ── JSON ──
  try {
    await c.jsonSet("userAI", "$", "asdas");
    console.log("jsonset: OK");
  } catch (e) {
    console.log(`jsonset error: ${e}`);
  }

  const res = await c.jsonGet("userAI");
  console.log(`json get response: ${res}`);
} finally {
  c.close();
}
