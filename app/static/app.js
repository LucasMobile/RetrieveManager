(() => {
  "use strict";

  const body = document.body;
  const sidebar = document.querySelector(".sidebar");
  const menuButton = document.querySelector("[data-sidebar-open]");

  const setSidebar = (open) => {
    body.classList.toggle("sidebar-is-open", open);
    menuButton?.setAttribute("aria-expanded", String(open));
    if (open) {
      sidebar?.querySelector("a, button")?.focus();
    } else {
      menuButton?.focus();
    }
  };

  menuButton?.addEventListener("click", () => setSidebar(true));
  document.querySelectorAll("[data-sidebar-close]").forEach((button) => {
    button.addEventListener("click", () => setSidebar(false));
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && body.classList.contains("sidebar-is-open")) {
      setSidebar(false);
    }
  });

  document.querySelectorAll("[data-toast-close]").forEach((button) => {
    button.addEventListener("click", () => button.closest("[data-toast]")?.remove());
  });

  document.querySelectorAll("[data-password-toggle]").forEach((button) => {
    const input = document.getElementById(button.dataset.passwordToggle);
    if (!input) return;
    button.addEventListener("click", () => {
      const reveal = input.type === "password";
      input.type = reveal ? "text" : "password";
      button.textContent = reveal ? "Ocultar" : "Mostrar";
      button.setAttribute("aria-pressed", String(reveal));
    });
  });

  const markSubmitting = (form) => {
    if (form.dataset.submitting === "true") return;
    form.dataset.submitting = "true";
    form.setAttribute("aria-busy", "true");
    const submitter = form.querySelector("button[type='submit'], input[type='submit']");
    if (submitter) {
      submitter.classList.add("is-loading");
      submitter.setAttribute("aria-disabled", "true");
      submitter.disabled = true;
    }
  };

  const dialog = document.querySelector("[data-confirm-dialog]");
  const dialogMessage = dialog?.querySelector("[data-confirm-message]");
  const confirmButton = dialog?.querySelector("[data-confirm-accept]");
  let pendingForm = null;

  document.addEventListener("submit", (event) => {
    const form = event.target;
    if (!(form instanceof HTMLFormElement)) return;

    if (form.dataset.confirm && form.dataset.confirmed !== "true") {
      event.preventDefault();
      if (!dialog || typeof dialog.showModal !== "function") {
        if (window.confirm(form.dataset.confirm)) {
          form.dataset.confirmed = "true";
          form.requestSubmit();
        }
        return;
      }
      pendingForm = form;
      if (dialogMessage) dialogMessage.textContent = form.dataset.confirm;
      if (confirmButton) {
        confirmButton.textContent = form.dataset.confirmLabel || "Confirmar";
      }
      dialog.showModal();
      confirmButton?.focus();
      return;
    }

    markSubmitting(form);
  });

  dialog?.querySelector("[data-confirm-cancel]")?.addEventListener("click", () => {
    pendingForm = null;
    dialog.close();
  });

  confirmButton?.addEventListener("click", () => {
    if (!pendingForm) return;
    const form = pendingForm;
    pendingForm = null;
    dialog.close();
    form.dataset.confirmed = "true";
    form.requestSubmit();
  });

  dialog?.addEventListener("close", () => {
    pendingForm = null;
  });

  const refreshPartial = async (element) => {
    if (element.dataset.loading === "true" || document.hidden) return;
    if (element.contains(document.activeElement)) return;

    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 8000);
    element.dataset.loading = "true";
    element.setAttribute("aria-busy", "true");

    try {
      const response = await fetch(element.dataset.poll, {
        headers: { "X-Partial": "1" },
        signal: controller.signal,
      });
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      element.innerHTML = await response.text();
      element.removeAttribute("data-poll-error");
    } catch (_error) {
      element.setAttribute("data-poll-error", "true");
    } finally {
      window.clearTimeout(timeout);
      element.dataset.loading = "false";
      element.setAttribute("aria-busy", "false");
    }
  };

  document.querySelectorAll("[data-poll]").forEach((element) => {
    const interval = Number.parseInt(element.dataset.interval || "5000", 10);
    window.setInterval(() => refreshPartial(element), Math.max(interval, 2000));
  });

  const unitForm = document.querySelector("[data-unit-form]");
  const echoPanel = unitForm?.querySelector("[data-echo-test]");
  const echoButton = echoPanel?.querySelector("[data-echo-button]");
  const echoStatus = echoPanel?.querySelector("[data-echo-status]");
  const connectionFields = unitForm?.querySelectorAll("[data-connection-field]") || [];

  const setEchoStatus = (message, state = "") => {
    if (!echoStatus) return;
    echoStatus.textContent = message;
    echoStatus.classList.toggle("is-success", state === "success");
    echoStatus.classList.toggle("is-error", state === "error");
  };

  connectionFields.forEach((field) => {
    const output = unitForm?.querySelector(
      `[data-connection-output="${field.dataset.connectionField}"]`,
    );
    const syncConnection = () => {
      if (output) output.textContent = field.value.trim() || "—";
      setEchoStatus("Não testado");
    };
    field.addEventListener("input", syncConnection);
  });

  echoButton?.addEventListener("click", async () => {
    const invalidField = Array.from(connectionFields).find((field) => !field.checkValidity());
    if (invalidField) {
      invalidField.reportValidity();
      invalidField.focus();
      return;
    }

    const payload = new FormData();
    connectionFields.forEach((field) => payload.append(field.name, field.value));
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), 12000);
    echoButton.disabled = true;
    echoButton.classList.add("is-loading");
    setEchoStatus("Testando conexão…");

    try {
      const response = await fetch(echoPanel.dataset.url, {
        method: "POST",
        body: payload,
        headers: { Accept: "application/json" },
        signal: controller.signal,
      });
      const result = await response.json();
      setEchoStatus(result.message || "Não foi possível concluir o teste.", result.ok ? "success" : "error");
    } catch (error) {
      const message = error.name === "AbortError"
        ? "O teste excedeu o tempo limite."
        : "Falha de comunicação ao executar o teste.";
      setEchoStatus(message, "error");
    } finally {
      window.clearTimeout(timeout);
      echoButton.disabled = false;
      echoButton.classList.remove("is-loading");
    }
  });

  document.querySelectorAll("[data-second-rule]").forEach((rule) => {
    const toggle = rule.querySelector("[data-second-toggle]");
    const value = rule.querySelector("[data-second-value]");
    const field = rule.querySelector("[data-second-field]");
    const input = rule.querySelector("[data-second-input]");
    const label = rule.querySelector("[data-second-label]");
    const description = rule.querySelector("[data-second-description]");
    const attempt = toggle?.closest(".retrieve-attempt");
    if (!toggle || !value || !field || !input || !label || !description) return;

    const setSecondRetrieve = (active) => {
      toggle.setAttribute("aria-checked", String(active));
      toggle.setAttribute(
        "aria-label",
        `${active ? "Desativar" : "Ativar"} segunda tentativa`,
      );
      value.value = active ? "1" : "0";
      input.disabled = !active;
      field.classList.toggle("is-disabled", !active);
      attempt?.classList.toggle("is-off", !active);
      label.textContent = active ? "Ativada" : "Desativada";
      description.textContent = active ? "Nova busca automática" : "Uma única busca";
    };

    toggle.addEventListener("click", () => {
      setSecondRetrieve(toggle.getAttribute("aria-checked") !== "true");
    });
  });
})();
