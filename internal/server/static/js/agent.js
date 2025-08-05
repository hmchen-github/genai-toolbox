const log   = document.getElementById("chat-log");
const form  = document.getElementById("chat-form");
const input = document.getElementById("msg");
let   es;          // EventSource

form.onsubmit = async (e) => {
  e.preventDefault();
  const txt = input.value.trim();
  if (!txt) return;
  append("user", txt);

  // start conversation
  const res  = await fetch("/ui/chat", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ message: txt })
  });
  const { id } = await res.json();

  // subscribe to SSE
  es?.close();
  es = new EventSource(`/ui/chat/${id}/events`);

  // ---------- connection‑level errors ----------
  es.addEventListener("error", (ev) => {
    if (es.readyState === EventSource.CLOSED) return;
    console.error("EventSource connection error", ev);
    append("error", { content: "lost connection to server" });
  });

  // ---------- server‑sent events we expect ----------
  ["assistant", "tool_call", "tool_resp", "agent_error", "done"].forEach(type => {
    es.addEventListener(type, (ev) => {
      if (type === "done") {
        append("assistant", { content: "✓ conversation finished" });
        es.close();                      
        return;
      }
      const data = JSON.parse(ev.data);
      append(type, data);
    });
  });

  input.value = "";
};

function append(type, payload) {
  const div = document.createElement("div");
  div.className = type;
  switch (type) {
    case "tool_call":
      div.textContent = `${payload.toolName}(${JSON.stringify(payload.arguments)})`;
      break;
    case "tool_resp":
      div.textContent = `${payload.toolName} → ${JSON.stringify(payload.content)}`;
      break;
    default:
      div.textContent = (payload.content ?? payload);
  }
  log.appendChild(div);
  log.scrollTop = log.scrollHeight; // auto‑scroll
}
