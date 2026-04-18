const API_URL = "http://localhost:8000/query";
const chat = document.getElementById("chat");
const input = document.getElementById("question-input");
const sendBtn = document.getElementById("send-btn");
const emptyState = document.getElementById("empty-state");

let isLoading = false;

input.addEventListener("keydown", function(e) {
    if (e.key === "Enter" && !e.shiftKey) {
        e.preventDefault();
        sendQuestion();
    }
});

input.addEventListener("input", function() {
    this.style.height = "auto";
    this.style.height = Math.min(this.scrollHeight, 120) + "px";
});

function fillQuestion(btn) {
    input.value = btn.textContent;
    input.focus();
}

function hideEmptyState() {
    if (emptyState) {
        emptyState.style.display = "none";
    }
}

function appendMessage(role, text) {
    hideEmptyState();

    const msg = document.createElement("div");
    msg.className = "message " + role;

    const label = document.createElement("div");
    label.className = "message-label";
    label.textContent = role === "user" ? "You" : "LawFlow";

    const bubble = document.createElement("div");
    bubble.className = "message-bubble";
    bubble.textContent = text;

    msg.appendChild(label);
    msg.appendChild(bubble);
    chat.appendChild(msg);
    chat.scrollTop = chat.scrollHeight;
}

function appendThinking() {
    hideEmptyState();

    const msg = document.createElement("div");
    msg.className = "message assistant";
    msg.id = "thinking-msg";

    const label = document.createElement("div");
    label.className = "message-label";
    label.textContent = "LawFlow";

    const thinking = document.createElement("div");
    thinking.className = "thinking";
    thinking.innerHTML = `
        <span>Searching regulations</span>
        <div class="dots">
            <span></span><span></span><span></span>
        </div>
    `;

    msg.appendChild(label);
    msg.appendChild(thinking);
    chat.appendChild(msg);
    chat.scrollTop = chat.scrollHeight;
}

function removeThinking() {
    const el = document.getElementById("thinking-msg");
    if (el) el.remove();
}

function appendError(message) {
    hideEmptyState();

    const err = document.createElement("div");
    err.className = "error-bubble";
    err.textContent = message;
    chat.appendChild(err);
    chat.scrollTop = chat.scrollHeight;
}

async function sendQuestion() {
    const question = input.value.trim();
    if (!question || isLoading) return;

    isLoading = true;
    sendBtn.disabled = true;
    input.value = "";
    input.style.height = "auto";

    appendMessage("user", question);
    appendThinking();

    try {
        const response = await fetch(API_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ question }),
        });

        removeThinking();

        if (!response.ok) {
            const error = await response.json();
            appendError("Error: " + (error.detail || "Something went wrong"));
            return;
        }

        const data = await response.json();
        appendMessage("assistant", data.answer);

    } catch (err) {
        removeThinking();
        appendError("Could not connect to LawFlow API. Make sure the server is running on localhost:8000");
    } finally {
        isLoading = false;
        sendBtn.disabled = false;
        input.focus();
    }
}