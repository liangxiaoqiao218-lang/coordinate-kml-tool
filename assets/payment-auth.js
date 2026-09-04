const root = document.querySelector("[data-payment-auth-root]");

if (root) {
  const status = root.querySelector("[data-auth-status]");
  const openButton = root.querySelector("[data-auth-open]");
  const logoutButton = root.querySelector("[data-auth-logout]");
  const dialog = document.querySelector("[data-auth-dialog]");
  const closeButton = dialog.querySelector("[data-auth-close]");
  const emailForm = dialog.querySelector("[data-auth-email-form]");
  const otpForm = dialog.querySelector("[data-auth-otp-form]");
  const emailInput = dialog.querySelector("[data-auth-email]");
  const otpInput = dialog.querySelector("[data-auth-otp]");
  const message = dialog.querySelector("[data-auth-message]");
  let pendingEmail = "";
  let currentUser = null;

  function publish(state, user = null) {
    if (state === "authenticated") currentUser = user;
    else if (state !== "unknown") currentUser = null;
    root.dataset.authState = state;
    const authenticated = state === "authenticated";
    const uncertain = state === "unknown";
    status.textContent = authenticated
      ? (currentUser?.email || "已登录")
      : uncertain
        ? "退出状态未确认，请重试"
        : state === "expired" ? "登录已过期" : "未登录";
    openButton.hidden = authenticated || uncertain;
    logoutButton.hidden = !authenticated && !uncertain;
    window.dispatchEvent(new CustomEvent("geokit:payment-auth-state", { detail: { state, user: currentUser } }));
  }

  function setMessage(text, isError = false) {
    message.textContent = text;
    message.dataset.error = String(isError);
  }

  async function api(path, options = {}) {
    const response = await fetch(path, {
      ...options,
      credentials: "include",
      headers: { "Content-Type": "application/json", ...(options.headers || {}) }
    });
    const payload = await response.json().catch(() => ({ success: false, code: "AUTH_RESPONSE_INVALID" }));
    if (!response.ok) {
      const error = new Error(payload.code || "AUTH_REQUEST_FAILED");
      error.status = response.status;
      throw error;
    }
    return payload;
  }

  async function loadSession() {
    try {
      const payload = await api("/api/auth/me", { method: "GET" });
      if (payload.state === "authenticated") {
        publish(payload.state, payload.user);
        return;
      }
      try {
        await refreshSession("signed_out");
      } catch (_) {}
    } catch (error) {
      if (error.status === 401) {
        try {
          await refreshSession("expired");
        } catch (_) {}
      } else {
        publish("unknown");
      }
    }
  }

  async function refreshSession(failureState = "expired") {
    try {
      const payload = await api("/api/auth/refresh", { method: "POST", body: "{}" });
      publish("authenticated", payload.user);
      return payload;
    } catch (error) {
      publish(error.status === 401 || error.status === 503 ? failureState : "unknown");
      throw error;
    }
  }

  openButton.addEventListener("click", () => {
    setMessage("");
    dialog.showModal();
    emailInput.focus();
  });
  closeButton.addEventListener("click", () => dialog.close());

  emailForm.addEventListener("submit", async event => {
    event.preventDefault();
    const submit = emailForm.querySelector("button[type=submit]");
    submit.disabled = true;
    setMessage("正在发送验证码…");
    try {
      pendingEmail = emailInput.value.trim();
      await api("/api/auth/otp/request", { method: "POST", body: JSON.stringify({ email: pendingEmail }) });
      emailForm.hidden = true;
      otpForm.hidden = false;
      otpInput.focus();
      publish("pending_otp");
      setMessage("如果邮箱可用，验证码已发送。请检查收件箱。");
    } catch (_) {
      setMessage("暂时无法发送验证码，请稍后再试。", true);
    } finally {
      submit.disabled = false;
    }
  });

  otpForm.addEventListener("submit", async event => {
    event.preventDefault();
    const submit = otpForm.querySelector("button[type=submit]");
    submit.disabled = true;
    setMessage("正在验证…");
    try {
      const payload = await api("/api/auth/otp/verify", {
        method: "POST",
        body: JSON.stringify({ email: pendingEmail, token: otpInput.value.trim() })
      });
      publish(payload.state, payload.user);
      dialog.close();
      otpForm.reset();
      otpForm.hidden = true;
      emailForm.hidden = false;
    } catch (_) {
      setMessage("验证码无效或已过期，请重试。", true);
    } finally {
      submit.disabled = false;
    }
  });

  logoutButton.addEventListener("click", async () => {
    logoutButton.disabled = true;
    try {
      const payload = await api("/api/auth/logout", { method: "POST", body: "{}" });
      if (payload.state !== "signed_out") throw new Error("AUTH_LOGOUT_UNCONFIRMED");
      currentUser = null;
      publish("signed_out");
    } catch (_) {
      publish("unknown", currentUser);
    } finally {
      logoutButton.disabled = false;
    }
  });

  window.geokitPaymentAuth = Object.freeze({
    refresh: () => refreshSession("expired")
  });

  loadSession();
}
