(() => {
  "use strict";

  const body = document.body;
  const root = document.documentElement;
  const sidebar = document.querySelector(".sidebar");
  const menuButton = document.querySelector("[data-sidebar-open]");
  const themeToggle = document.querySelector("[data-theme-toggle]");

  const parseCsv = (value) =>
    (value || "").split(",").map((item) => item.trim()).filter(Boolean);
  const sortedValues = (values) =>
    Array.from(values).sort((left, right) => left.localeCompare(right));
  const DICOM_MODALITY = /^[A-Z0-9]{1,8}$/;

  // Busca e lê a resposta sob um único prazo: `read` também é abortado, então
  // uma resposta travada não deixa quem chamou esperando para sempre.
  const fetchWithTimeout = async (url, options, timeoutMs, read) => {
    const controller = new AbortController();
    const timeout = window.setTimeout(() => controller.abort(), timeoutMs);
    try {
      return await read(await fetch(url, { ...options, signal: controller.signal }));
    } finally {
      window.clearTimeout(timeout);
    }
  };

  const modalityChip = (text, { onRemove = null, removeLabel = "", disabled = false } = {}) => {
    const chip = document.createElement("span");
    chip.className = "modality-selection-chip";
    chip.textContent = text;
    if (onRemove) {
      const remove = document.createElement("button");
      remove.type = "button";
      remove.disabled = disabled;
      remove.setAttribute("aria-label", removeLabel);
      remove.textContent = "×";
      remove.addEventListener("click", onRemove);
      chip.append(remove);
    }
    return chip;
  };

  // Campo de texto que transforma modalidades DICOM digitadas em chips.
  // `values` é o conjunto exibido; `render` redesenha após cada mudança.
  const bindModalityInput = (
    input,
    { values, render, onAdd = (modality) => values.add(modality), canPop = () => values.size > 0 },
  ) => {
    const add = () => {
      if (!input || input.disabled) return true;
      const modality = input.value.trim().toUpperCase();
      if (!modality) return true;
      if (!DICOM_MODALITY.test(modality)) {
        input.setCustomValidity("Use um código DICOM com até 8 letras ou números.");
        input.reportValidity();
        return false;
      }
      input.setCustomValidity("");
      onAdd(modality);
      input.value = "";
      render();
      return true;
    };
    input?.addEventListener("input", () => input.setCustomValidity(""));
    input?.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === ",") {
        event.preventDefault();
        add();
      } else if (event.key === "Backspace" && !input.value && canPop()) {
        values.delete(sortedValues(values).at(-1));
        render();
      }
    });
    input?.addEventListener("blur", add);
    // O texto ainda não confirmado vira chip antes do envio; inválido bloqueia.
    input?.form?.addEventListener("submit", (event) => {
      if (!add()) event.preventDefault();
    });
  };

  const syncThemeToggle = () => {
    const isDark = root.dataset.theme === "dark";
    const label = themeToggle?.querySelector("[data-theme-label]");
    const sun = themeToggle?.querySelector(".theme-toggle__icon--sun");
    const moon = themeToggle?.querySelector(".theme-toggle__icon--moon");
    const action = isDark ? "Ativar tema claro" : "Ativar tema escuro";
    if (label) label.textContent = isDark ? "Tema claro" : "Tema escuro";
    if (sun) sun.hidden = !isDark;
    if (moon) moon.hidden = isDark;
    themeToggle?.setAttribute("aria-label", action);
    themeToggle?.setAttribute("title", action);
    document.querySelector('meta[name="theme-color"]')?.setAttribute("content", isDark ? "#0b1725" : "#071b33");
  };

  themeToggle?.addEventListener("click", () => {
    const theme = root.dataset.theme === "dark" ? "light" : "dark";
    root.dataset.theme = theme;
    localStorage.setItem("rm-theme", theme);
    syncThemeToggle();
  });
  syncThemeToggle();

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

  const customSelects = () => Array.from(document.querySelectorAll("[data-custom-select]"));
  // Fixed to the viewport so cards with overflow: hidden never clip the menu;
  // it opens upwards when there is no room below the trigger.
  const placeCustomSelectMenu = (trigger, menu) => {
    const rect = trigger.getBoundingClientRect();
    const gap = 7;
    const height = Math.min(menu.scrollHeight, 288);
    const below = window.innerHeight - rect.bottom;
    menu.style.position = "fixed";
    menu.style.left = `${rect.left}px`;
    menu.style.right = "auto";
    menu.style.width = `${rect.width}px`;
    if (below < height + gap * 2 && rect.top > below) {
      menu.style.top = "auto";
      menu.style.bottom = `${window.innerHeight - rect.top + gap}px`;
    } else {
      menu.style.top = `${rect.bottom + gap}px`;
      menu.style.bottom = "auto";
    }
  };
  const setCustomSelectOpen = (select, open, restoreFocus = false) => {
    const trigger = select?.querySelector("[data-custom-select-trigger]");
    const menu = select?.querySelector("[data-custom-select-menu]");
    if (!trigger || !menu) return;
    trigger.setAttribute("aria-expanded", String(open));
    menu.hidden = !open;
    select.classList.toggle("is-open", open);
    if (open) placeCustomSelectMenu(trigger, menu);
    if (open) {
      const selected = menu.querySelector('[aria-selected="true"]');
      (selected || menu.querySelector("[data-custom-select-option]"))?.focus();
    } else if (restoreFocus) {
      trigger.focus();
    }
  };

  const closeCustomSelects = (except = null) => {
    customSelects().forEach((select) => {
      if (select !== except) setCustomSelectOpen(select, false);
    });
  };

  const bindCustomSelect = (select) => {
    if (select.dataset.customSelectBound) return;
    select.dataset.customSelectBound = "1";
    const trigger = select.querySelector("[data-custom-select-trigger]");
    const menu = select.querySelector("[data-custom-select-menu]");
    const value = select.querySelector("[data-custom-select-value]");
    const label = select.querySelector("[data-custom-select-label]");
    const options = Array.from(select.querySelectorAll("[data-custom-select-option]"));

    trigger?.addEventListener("click", () => {
      const open = trigger.getAttribute("aria-expanded") !== "true";
      closeCustomSelects(select);
      setCustomSelectOpen(select, open);
    });

    options.forEach((option, index) => {
      option.addEventListener("click", () => {
        if (value) value.value = option.dataset.value || "";
        if (label) label.textContent = option.querySelector("span")?.textContent || option.textContent.trim();
        options.forEach((item) => item.setAttribute("aria-selected", String(item === option)));
        setCustomSelectOpen(select, false, true);
        const native = select.nativeSelect;
        if (native && native.value !== (option.dataset.value || "")) {
          native.value = option.dataset.value || "";
          native.dispatchEvent(new Event("change", { bubbles: true }));
        }
        if (select.hasAttribute("data-custom-select-submit")) select.closest("form")?.requestSubmit();
      });
      option.addEventListener("keydown", (event) => {
        if (event.key === "ArrowDown" || event.key === "ArrowUp") {
          event.preventDefault();
          const direction = event.key === "ArrowDown" ? 1 : -1;
          options[(index + direction + options.length) % options.length]?.focus();
        }
        if (event.key === "Home" || event.key === "End") {
          event.preventDefault();
          options[event.key === "Home" ? 0 : options.length - 1]?.focus();
        }
      });
    });

    trigger?.addEventListener("keydown", (event) => {
      if (event.key === "ArrowDown" || event.key === "ArrowUp") {
        event.preventDefault();
        closeCustomSelects(select);
        setCustomSelectOpen(select, true);
      }
    });

    menu?.addEventListener("keydown", (event) => {
      if (event.key === "Escape") {
        event.preventDefault();
        setCustomSelectOpen(select, false, true);
      }
    });
  };
  const initCustomSelects = (scope = document) =>
    scope.querySelectorAll("[data-custom-select]").forEach(bindCustomSelect);

  // Every native <select> gets the themed dropdown. The native element stays
  // in the form, hidden, and keeps its name, value and "change" listeners.
  const svgIcon = (path) =>
    `<svg class="icon" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true" focusable="false"><path d="${path}" /></svg>`;
  let nativeSelectCount = 0;
  const enhanceNativeSelect = (native) => {
    if (native.dataset.nativeSelect !== undefined || native.multiple || native.size > 1) return;
    native.dataset.nativeSelect = "enhanced";
    nativeSelectCount += 1;
    const baseId = native.id || `select-${nativeSelectCount}`;
    const label = native.id ? document.querySelector(`label[for="${native.id}"]`) : null;
    const labelId = label ? label.id || `${baseId}-label` : "";
    if (label) label.id = labelId;

    const wrapper = document.createElement("div");
    wrapper.className = "custom-select custom-select--native";
    wrapper.setAttribute("data-custom-select", "");
    wrapper.nativeSelect = native;

    const trigger = document.createElement("button");
    trigger.type = "button";
    trigger.className = "custom-select__trigger";
    trigger.setAttribute("aria-haspopup", "listbox");
    trigger.setAttribute("aria-expanded", "false");
    trigger.setAttribute("data-custom-select-trigger", "");
    trigger.disabled = native.disabled;
    const current = document.createElement("span");
    current.id = `${baseId}-value`;
    current.setAttribute("data-custom-select-label", "");
    trigger.append(current);
    trigger.insertAdjacentHTML("beforeend", svgIcon("m6 9 6 6 6-6"));
    if (labelId) trigger.setAttribute("aria-labelledby", `${labelId} ${current.id}`);
    else if (native.getAttribute("aria-label")) trigger.setAttribute("aria-label", native.getAttribute("aria-label"));

    const menu = document.createElement("div");
    menu.className = "custom-select__menu";
    menu.setAttribute("role", "listbox");
    menu.setAttribute("data-custom-select-menu", "");
    if (labelId) menu.setAttribute("aria-labelledby", labelId);
    menu.hidden = true;
    Array.from(native.options).forEach((item) => {
      const option = document.createElement("button");
      option.type = "button";
      option.className = "custom-select__option";
      option.setAttribute("role", "option");
      option.setAttribute("data-custom-select-option", "");
      option.dataset.value = item.value;
      option.disabled = item.disabled;
      option.setAttribute("aria-selected", String(item.selected));
      const text = document.createElement("span");
      text.textContent = item.textContent.trim();
      option.append(text);
      option.insertAdjacentHTML("beforeend", svgIcon("m5 12 4 4L19 6"));
      menu.append(option);
    });
    current.textContent = native.selectedOptions[0]?.textContent.trim() || "";

    // The label now points at the visible trigger; the native id stays for scripts.
    trigger.id = `${baseId}-trigger`;
    if (label) label.htmlFor = trigger.id;
    native.classList.add("custom-select__native");
    native.tabIndex = -1;
    native.setAttribute("aria-hidden", "true");
    wrapper.append(trigger, menu);
    native.after(wrapper);
    bindCustomSelect(wrapper);
  };
  const enhanceNativeSelects = (scope = document) => {
    if (scope.matches?.("select")) enhanceNativeSelect(scope);
    scope.querySelectorAll?.("select").forEach(enhanceNativeSelect);
  };
  enhanceNativeSelects();
  initCustomSelects();
  // Rows added later (e.g. DICOM rule conditions) are enhanced as they appear.
  new MutationObserver((mutations) => {
    mutations.forEach((mutation) => mutation.addedNodes.forEach((node) => {
      if (node.nodeType === Node.ELEMENT_NODE) enhanceNativeSelects(node);
    }));
  }).observe(document.body, { childList: true, subtree: true });

  document.addEventListener("click", (event) => {
    if (!event.target.closest("[data-custom-select]")) closeCustomSelects();
  });
  // A fixed menu would drift away from its trigger: close it instead.
  window.addEventListener("resize", () => closeCustomSelects());
  window.addEventListener(
    "scroll",
    (event) => {
      if (!event.target.closest?.("[data-custom-select-menu]")) closeCustomSelects();
    },
    true,
  );

  // Botão que abre um painel: fecha com clique fora ou Esc (devolvendo o foco).
  const disclosures = [];
  const bindDisclosure = (root, toggle, panel, { focusFirst = false } = {}) => {
    if (!root || !toggle || !panel) return null;
    const isOpen = () => toggle.getAttribute("aria-expanded") === "true";
    const set = (open, restoreFocus = false) => {
      toggle.setAttribute("aria-expanded", String(open));
      panel.hidden = !open;
      if (open && focusFirst) panel.querySelector("button:not([disabled])")?.focus();
      if (restoreFocus) toggle.focus();
    };
    toggle.addEventListener("click", () => set(!isOpen()));
    const disclosure = { root, isOpen, set };
    disclosures.push(disclosure);
    return disclosure;
  };

  const accountMenu = document.querySelector("[data-account-menu]");
  bindDisclosure(
    accountMenu,
    accountMenu?.querySelector("[data-account-menu-toggle]"),
    accountMenu?.querySelector("[data-account-menu-panel]"),
  );

  document.querySelectorAll("[data-menu]").forEach((menu) => {
    const actionMenu = bindDisclosure(
      menu,
      menu.querySelector("[data-menu-toggle]"),
      menu.querySelector("[data-menu-panel]"),
      { focusFirst: true },
    );
    // Fecha antes do diálogo de confirmação abrir sobre o menu.
    menu.addEventListener("submit", () => actionMenu?.set(false));
  });

  document.addEventListener("click", (event) => {
    disclosures.forEach(({ root, set }) => {
      if (!root.contains(event.target)) set(false);
    });
  });

  document.addEventListener("keydown", (event) => {
    if (event.key !== "Escape") return;
    if (body.classList.contains("sidebar-is-open")) setSidebar(false);
    disclosures.forEach(({ isOpen, set }) => {
      if (isOpen()) set(false, true);
    });
  });

  document.querySelectorAll("[data-toast-close]").forEach((button) => {
    button.addEventListener("click", () => button.closest("[data-toast]")?.remove());
  });

  document.querySelectorAll("[data-password-toggle]").forEach((button) => {
    const input = document.getElementById(button.dataset.passwordToggle);
    const showIcon = button.querySelector("[data-password-show]");
    const hideIcon = button.querySelector("[data-password-hide]");
    if (!input) return;
    button.addEventListener("click", () => {
      const reveal = input.type === "password";
      input.type = reveal ? "text" : "password";
      if (showIcon) showIcon.hidden = reveal;
      if (hideIcon) hideIcon.hidden = !reveal;
      const action = reveal ? "Ocultar" : "Mostrar";
      const passwordLabel = button.dataset.passwordLabel || "senha";
      button.setAttribute("aria-label", `${action} ${passwordLabel}`);
      button.setAttribute("title", `${action} senha`);
      button.setAttribute("aria-pressed", String(reveal));
    });
  });

  const newPassword = document.querySelector("#new-password");
  const validatePassword = () => {
    const bytes = new TextEncoder().encode(newPassword.value).length;
    newPassword.setCustomValidity(
      bytes >= 12 && bytes <= 72 ? "" : "A senha deve ter entre 12 e 72 bytes.",
    );
  };
  newPassword?.addEventListener("input", validatePassword);
  newPassword?.addEventListener("change", validatePassword);

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

  // Idade do último conteúdo recebido. O texto muda em degraus de 10 s para
  // não repetir anúncios na região aria-live enquanto o polling está em dia.
  const formatAge = (seconds) => {
    if (seconds < 10) return "agora";
    if (seconds < 60) return `há ${Math.floor(seconds / 10) * 10} s`;
    return `há ${Math.floor(seconds / 60)} min`;
  };

  const renderPollAge = (element) => {
    const updatedAt = Number(element.dataset.updatedAt || Date.now());
    const interval = Number.parseInt(element.dataset.interval || "5000", 10);
    const ageMs = Date.now() - updatedAt;
    element.classList.toggle("is-stale", ageMs > interval * 3);
    const age = formatAge(Math.floor(ageMs / 1000));
    element.querySelectorAll("[data-age]").forEach((node) => {
      const text = node.dataset.age ? `${node.dataset.age} ${age}` : age.charAt(0).toUpperCase() + age.slice(1);
      if (node.textContent !== text) node.textContent = text;
    });
  };

  // Regiões com [data-poll-slot] trocam só esses blocos (o resto, como um
  // formulário de filtros, fica intacto); as demais trocam todo o conteúdo.
  const applyPartial = (element, html) => {
    const slots = element.querySelectorAll("[data-poll-slot]");
    if (!slots.length) {
      element.innerHTML = html;
      return;
    }
    const incoming = new DOMParser().parseFromString(html, "text/html");
    slots.forEach((slot) => {
      const next = incoming.querySelector(`[data-poll-slot="${slot.dataset.pollSlot}"]`);
      if (next) slot.replaceWith(document.importNode(next, true));
    });
  };

  const refreshPartial = async (element) => {
    if (element.dataset.loading === "true" || document.hidden) return;
    if (element.contains(document.activeElement)) return;
    // Uma confirmação aberta aponta para um formulário da região; trocá-lo
    // agora faria o "Confirmar" enviar um formulário que saiu da página.
    if (document.querySelector("dialog[open]")) return;

    element.dataset.loading = "true";
    element.setAttribute("aria-busy", "true");

    try {
      const html = await fetchWithTimeout(
        element.dataset.poll,
        { headers: { "X-Partial": "1" } },
        8000,
        (response) => {
          if (!response.ok) throw new Error(`HTTP ${response.status}`);
          // Sessão expirada redireciona para o login; não injetar essa página na região.
          if (response.redirected) throw new Error("redirected");
          return response.text();
        },
      );
      applyPartial(element, html);
      initCustomSelects(element);
      element.removeAttribute("data-poll-error");
      element.dataset.updatedAt = String(Date.now());
      renderPollAge(element);
    } catch (_error) {
      element.setAttribute("data-poll-error", "true");
    } finally {
      element.dataset.loading = "false";
      element.setAttribute("aria-busy", "false");
    }
  };

  const pollRegions = [...document.querySelectorAll("[data-poll]")];
  pollRegions.forEach((element) => {
    const interval = Number.parseInt(element.dataset.interval || "5000", 10);
    element.dataset.updatedAt = String(Date.now());
    window.setInterval(() => refreshPartial(element), Math.max(interval, 2000));
  });
  if (pollRegions.some((element) => element.querySelector("[data-age]"))) {
    window.setInterval(() => pollRegions.forEach(renderPollAge), 1000);
  }
  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) pollRegions.forEach(refreshPartial);
  });

  // Índice de seções: marca como atual a última seção cujo topo passou de
  // 35% da altura da janela; no fim da página, a última seção.
  document.querySelectorAll("[data-section-nav]").forEach((nav) => {
    const entries = [...nav.querySelectorAll('a[href^="#"]')]
      .map((link) => {
        const target = document.getElementById(link.getAttribute("href").slice(1));
        return target ? { link, section: target.closest("section") || target } : null;
      })
      .filter(Boolean);
    if (!entries.length) return;

    let current = null;
    const update = () => {
      const threshold = window.innerHeight * 0.35;
      const atBottom =
        window.innerHeight + window.scrollY >= document.documentElement.scrollHeight - 2;
      let next = entries[0];
      if (atBottom) {
        next = entries[entries.length - 1];
      } else {
        entries.forEach((entry) => {
          if (entry.section.getBoundingClientRect().top <= threshold) next = entry;
        });
      }
      if (next === current) return;
      current?.link.removeAttribute("aria-current");
      next.link.setAttribute("aria-current", "location");
      current = next;
    };

    let frame = 0;
    const schedule = () => {
      if (frame) return;
      frame = window.requestAnimationFrame(() => {
        frame = 0;
        update();
      });
    };
    window.addEventListener("scroll", schedule, { passive: true });
    window.addEventListener("resize", schedule);
    update();
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
    echoButton.disabled = true;
    echoButton.classList.add("is-loading");
    setEchoStatus("Testando conexão…");

    try {
      const result = await fetchWithTimeout(
        echoPanel.dataset.url,
        {
          method: "POST",
          body: payload,
          headers: {
            Accept: "application/json",
            "X-CSRF-Token": unitForm.elements.namedItem("csrf_token").value,
          },
        },
        12000,
        (response) => response.json(),
      );
      setEchoStatus(result.ok ? "C-ECHO OK" : "FALHA C-ECHO", result.ok ? "success" : "error");
    } catch (error) {
      setEchoStatus("FALHA C-ECHO", "error");
    } finally {
      echoButton.disabled = false;
      echoButton.classList.remove("is-loading");
    }
  });

  const compressionPanel = unitForm?.querySelector("[data-unit-compression]");
  if (compressionPanel) {
    const profileInputs = Array.from(
      compressionPanel.querySelectorAll("[data-profile-input]"),
    );
    const profileSets = new Map(
      profileInputs.map((input) => [input.dataset.profileInput, new Set(parseCsv(input.value))]),
    );
    const dropValue = compressionPanel.querySelector("[data-drop-value]");
    const dropInput = compressionPanel.querySelector("[data-drop-input]");
    const dropChips = compressionPanel.querySelector("[data-drop-chips]");
    const drops = new Set(parseCsv(dropValue?.value));

    const renderCompressionSettings = () => {
      profileInputs.forEach((input) => {
        const profile = input.dataset.profileInput;
        const values = profileSets.get(profile) || new Set();
        input.value = sortedValues(values).join(",");
        const container = compressionPanel.querySelector(`[data-profile-chips="${profile}"]`);
        const count = compressionPanel.querySelector(`[data-profile-count="${profile}"]`);
        const dialogCount = compressionPanel.querySelector(`[data-dialog-count="${profile}"]`);
        if (count) count.textContent = String(values.size);
        if (dialogCount) dialogCount.textContent = String(values.size);
        const dialogWord = compressionPanel.querySelector(`[data-dialog-count-word="${profile}"]`);
        if (dialogWord) dialogWord.textContent = values.size === 1 ? "selecionada" : "selecionadas";
        if (container) {
          container.replaceChildren();
          if (values.size === 0) {
            const empty = document.createElement("span");
            empty.className = "unit-compression-profile__empty";
            empty.textContent = "Nenhuma modalidade específica";
            container.append(empty);
          } else {
            sortedValues(values).forEach((value) => container.append(modalityChip(value)));
          }
        }
      });

      compressionPanel.querySelectorAll("[data-profile-option]").forEach((option) => {
        option.checked = profileSets.get(option.dataset.profileOption)?.has(option.value) || false;
      });
      compressionPanel.querySelectorAll("[data-profile-select-all]").forEach((toggle) => {
        const options = Array.from(
          compressionPanel.querySelectorAll(
            `[data-profile-option="${toggle.dataset.profileSelectAll}"]`,
          ),
        );
        const selected = options.filter((option) => option.checked).length;
        toggle.checked = options.length > 0 && selected === options.length;
        toggle.indeterminate = selected > 0 && selected < options.length;
      });

      if (dropValue) dropValue.value = sortedValues(drops).join(",");
      if (dropChips) {
        dropChips.replaceChildren();
        sortedValues(drops).forEach((value) =>
          dropChips.append(
            modalityChip(value, {
              removeLabel: `Remover ${value} do descarte`,
              onRemove: () => {
                drops.delete(value);
                renderCompressionSettings();
              },
            }),
          ),
        );
      }
    };

    // Cada modalidade fica em um único perfil ou no descarte.
    const assignProfile = (profile, modality, selected) => {
      const target = profileSets.get(profile);
      if (!target) return;
      if (selected) {
        profileSets.forEach((values, name) => {
          if (name !== profile) values.delete(modality);
        });
        drops.delete(modality);
        target.add(modality);
      } else {
        target.delete(modality);
      }
    };

    compressionPanel.querySelectorAll("[data-profile-option]").forEach((option) => {
      option.addEventListener("change", () => {
        assignProfile(option.dataset.profileOption, option.value, option.checked);
        renderCompressionSettings();
      });
    });
    compressionPanel.querySelectorAll("[data-profile-select-all]").forEach((toggle) => {
      toggle.addEventListener("change", () => {
        const profile = toggle.dataset.profileSelectAll;
        compressionPanel
          .querySelectorAll(`[data-profile-option="${profile}"]`)
          .forEach((option) => assignProfile(profile, option.value, toggle.checked));
        renderCompressionSettings();
      });
    });

    bindModalityInput(dropInput, {
      values: drops,
      render: renderCompressionSettings,
      onAdd: (modality) => {
        drops.add(modality);
        profileSets.forEach((values) => values.delete(modality));
      },
    });

    compressionPanel.querySelectorAll("[data-compression-open]").forEach((button) => {
      button.addEventListener("click", () => {
        const dialog = compressionPanel.querySelector(
          `[data-compression-dialog="${button.dataset.compressionOpen}"]`,
        );
        if (dialog?.showModal) dialog.showModal();
      });
    });
    compressionPanel.querySelectorAll("[data-compression-dialog]").forEach((dialog) => {
      dialog.querySelectorAll("[data-compression-close]").forEach((button) => {
        button.addEventListener("click", () => dialog.close());
      });
      dialog.addEventListener("click", (event) => {
        if (event.target === dialog) dialog.close();
      });
    });
    renderCompressionSettings();
  }

  const priorField = unitForm?.querySelector("[data-prior-modalities]");
  if (priorField) {
    const ALL = "ALL";
    const priorValue = priorField.querySelector("[data-prior-value]");
    const priorInput = priorField.querySelector("[data-prior-input]");
    const priorChips = priorField.querySelector("[data-prior-chips]");
    const priorToggle = unitForm.querySelector("#retrieve-prior-enabled");
    const priors = new Set(parseCsv(priorValue?.value));
    const priorEnabled = () => !priorToggle || priorToggle.checked;

    // ALL is exclusive: it stands alone and comes back when the list empties.
    const renderPriors = () => {
      if (!priors.size) priors.add(ALL);
      if (priorValue) priorValue.value = sortedValues(priors).join(",");
      const enabled = priorEnabled();
      priorField.classList.toggle("is-disabled", !enabled);
      if (priorInput) priorInput.disabled = !enabled;
      if (!priorChips) return;
      priorChips.replaceChildren();
      sortedValues(priors).forEach((value) => {
        priorChips.append(
          value === ALL
            ? modalityChip("ALL · todas")
            : modalityChip(value, {
                removeLabel: `Remover ${value} dos exames anteriores`,
                disabled: !enabled,
                onRemove: () => {
                  priors.delete(value);
                  renderPriors();
                },
              }),
        );
      });
    };

    bindModalityInput(priorInput, {
      values: priors,
      render: renderPriors,
      onAdd: (modality) => {
        if (modality === ALL) priors.clear();
        else priors.delete(ALL);
        priors.add(modality);
      },
      canPop: () => !priors.has(ALL),
    });
    priorToggle?.addEventListener("change", renderPriors);
    renderPriors();
  }

  document.querySelectorAll("[data-monitor-rule]").forEach((rule) => {
    const toggle = rule.querySelector("[data-monitor-toggle]");
    const value = rule.querySelector("[data-monitor-value]");
    const fields = rule.querySelectorAll("[data-monitor-field]");
    const inputs = rule.querySelectorAll("[data-monitor-input]");
    const label = rule.querySelector("[data-monitor-label]");
    const description = rule.querySelector("[data-monitor-description]");
    const attempt = toggle?.closest(".retrieve-attempt");
    if (!toggle || !value || !inputs.length || !label || !description) return;

    const setMonitoring = (active) => {
      toggle.setAttribute("aria-checked", String(active));
      toggle.setAttribute(
        "aria-label",
        `${active ? "Desativar" : "Ativar"} monitoramento de novas imagens`,
      );
      value.value = active ? "1" : "0";
      inputs.forEach((input) => {
        input.disabled = !active;
      });
      fields.forEach((field) => field.classList.toggle("is-disabled", !active));
      attempt?.classList.toggle("is-off", !active);
      label.textContent = active ? "Ativado" : "Desativado";
      description.textContent = active
        ? "Consulta periódica no PACS"
        : "Somente o 1º retrieve";
    };

    toggle.addEventListener("click", () => {
      setMonitoring(toggle.getAttribute("aria-checked") !== "true");
    });
  });

  const dicomRuleForm = document.querySelector("[data-dicom-rule-form]");
  if (dicomRuleForm) {
    const conditionList = dicomRuleForm.querySelector("[data-condition-list]");
    const conditionTemplate = document.querySelector("[data-condition-template]");
    const valuelessOperators = new Set(["exists", "not_exists"]);

    const updateConditionRows = () => {
      const rows = conditionList?.querySelectorAll("[data-condition-row]") || [];
      const combinator = dicomRuleForm.querySelector("#rule-combinator")?.value;
      rows.forEach((row, index) => {
        const button = row.querySelector("[data-condition-remove]");
        if (button) button.disabled = rows.length === 1;
        const number = row.querySelector("[data-condition-number]");
        if (number) number.textContent = String(index + 1);
        row.dataset.joinLabel = combinator === "or" ? "OU" : "E";
      });
    };

    const updateConditionValue = (row) => {
      const operator = row.querySelector("[data-condition-operator]");
      const value = row.querySelector("[data-condition-value]");
      if (!operator || !value) return;
      const valueless = valuelessOperators.has(operator.value);
      value.required = !valueless;
      value.readOnly = valueless;
      value.placeholder = valueless ? "Não se aplica" : "Valor para comparar";
      if (valueless) value.value = "";
    };

    const resolveTagName = async (input) => {
      const hint = input.closest(".field")?.querySelector("[data-tag-name]");
      const raw = input.value.trim();
      if (!hint || !raw) {
        if (hint) hint.textContent = "Informe uma tag padrão";
        return;
      }
      hint.textContent = "Validando tag…";
      try {
        const response = await fetch(`/rules/tag-info?tag=${encodeURIComponent(raw)}`);
        const result = await response.json();
        if (!response.ok || !result.ok) throw new Error(result.message || "Tag inválida");
        input.value = result.tag;
        input.setCustomValidity("");
        hint.textContent = result.name;
      } catch (error) {
        input.setCustomValidity(error.message || "Tag DICOM inválida");
        hint.textContent = error.message || "Tag DICOM inválida";
      }
    };

    const initializeConditionRow = (row) => {
      const operator = row.querySelector("[data-condition-operator]");
      operator?.addEventListener("change", () => updateConditionValue(row));
      row.querySelector("[data-condition-remove]")?.addEventListener("click", () => {
        row.remove();
        updateConditionRows();
      });
      updateConditionValue(row);
    };

    conditionList?.querySelectorAll("[data-condition-row]").forEach(initializeConditionRow);
    dicomRuleForm.querySelector("[data-condition-add]")?.addEventListener("click", () => {
      if (!conditionTemplate || !conditionList) return;
      if (conditionList.querySelectorAll("[data-condition-row]").length >= 20) return;
      const fragment = conditionTemplate.content.cloneNode(true);
      const row = fragment.querySelector("[data-condition-row]");
      initializeConditionRow(row);
      conditionList.appendChild(fragment);
      updateConditionRows();
      row.querySelector("[data-dicom-tag]")?.focus();
    });
    dicomRuleForm.querySelector("#rule-combinator")?.addEventListener("change", updateConditionRows);
    updateConditionRows();

    dicomRuleForm.addEventListener("focusout", (event) => {
      if (event.target.matches?.("[data-dicom-tag]")) resolveTagName(event.target);
    });
    dicomRuleForm.addEventListener("input", (event) => {
      if (event.target.matches?.("[data-dicom-tag]")) {
        event.target.setCustomValidity("");
      }
    });

    const action = dicomRuleForm.querySelector("[data-rule-action]");
    const actionTagField = dicomRuleForm.querySelector("[data-action-tag-field]");
    const actionValueField = dicomRuleForm.querySelector("[data-action-value-field]");
    const actionTag = actionTagField?.querySelector("input");
    const actionValue = actionValueField?.querySelector("input");
    const updateActionFields = () => {
      const changesTag = action?.value === "replace" || action?.value === "remove";
      const replaces = action?.value === "replace";
      if (actionTagField) actionTagField.hidden = !changesTag;
      if (actionValueField) actionValueField.hidden = !replaces;
      if (actionTag) actionTag.required = changesTag;
      if (actionValue) actionValue.required = replaces;
    };
    action?.addEventListener("change", updateActionFields);
    updateActionFields();
  }
})();
