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

  const accountMenu = document.querySelector("[data-account-menu]");
  const accountMenuToggle = accountMenu?.querySelector("[data-account-menu-toggle]");
  const accountMenuPanel = accountMenu?.querySelector("[data-account-menu-panel]");
  const setAccountMenu = (open, restoreFocus = false) => {
    if (!accountMenuToggle || !accountMenuPanel) return;
    accountMenuToggle.setAttribute("aria-expanded", String(open));
    accountMenuPanel.hidden = !open;
    if (restoreFocus) accountMenuToggle.focus();
  };

  accountMenuToggle?.addEventListener("click", () => {
    setAccountMenu(accountMenuToggle.getAttribute("aria-expanded") !== "true");
  });

  document.addEventListener("click", (event) => {
    if (accountMenu && !accountMenu.contains(event.target)) setAccountMenu(false);
  });

  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && body.classList.contains("sidebar-is-open")) {
      setSidebar(false);
    }
    if (event.key === "Escape" && accountMenuToggle?.getAttribute("aria-expanded") === "true") {
      setAccountMenu(false, true);
    }
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
        headers: {
          Accept: "application/json",
          "X-CSRF-Token": unitForm.elements.namedItem("csrf_token").value,
        },
        signal: controller.signal,
      });
      const result = await response.json();
      setEchoStatus(result.ok ? "C-ECHO OK" : "FALHA C-ECHO", result.ok ? "success" : "error");
    } catch (error) {
      setEchoStatus("FALHA C-ECHO", "error");
    } finally {
      window.clearTimeout(timeout);
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
      profileInputs.map((input) => [
        input.dataset.profileInput,
        new Set(input.value.split(",").map((value) => value.trim()).filter(Boolean)),
      ]),
    );
    const dropValue = compressionPanel.querySelector("[data-drop-value]");
    const dropInput = compressionPanel.querySelector("[data-drop-input]");
    const dropChips = compressionPanel.querySelector("[data-drop-chips]");
    const drops = new Set(
      (dropValue?.value || "").split(",").map((value) => value.trim()).filter(Boolean),
    );

    const sorted = (values) => Array.from(values).sort((left, right) => left.localeCompare(right));
    const makeChip = (value, removable = false) => {
      const chip = document.createElement("span");
      chip.className = "modality-selection-chip";
      chip.textContent = value;
      if (removable) {
        const remove = document.createElement("button");
        remove.type = "button";
        remove.setAttribute("aria-label", `Remover ${value} do descarte`);
        remove.textContent = "×";
        remove.addEventListener("click", () => {
          drops.delete(value);
          renderCompressionSettings();
        });
        chip.append(remove);
      }
      return chip;
    };

    const renderCompressionSettings = () => {
      profileInputs.forEach((input) => {
        const profile = input.dataset.profileInput;
        const values = profileSets.get(profile) || new Set();
        input.value = sorted(values).join(",");
        const container = compressionPanel.querySelector(`[data-profile-chips="${profile}"]`);
        const count = compressionPanel.querySelector(`[data-profile-count="${profile}"]`);
        const dialogCount = compressionPanel.querySelector(`[data-dialog-count="${profile}"]`);
        if (count) count.textContent = String(values.size);
        if (dialogCount) dialogCount.textContent = String(values.size);
        if (container) {
          container.replaceChildren();
          if (values.size === 0) {
            const empty = document.createElement("span");
            empty.className = "unit-compression-profile__empty";
            empty.textContent = "Nenhuma modalidade específica";
            container.append(empty);
          } else {
            sorted(values).forEach((value) => container.append(makeChip(value)));
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

      if (dropValue) dropValue.value = sorted(drops).join(",");
      if (dropChips) {
        dropChips.replaceChildren();
        sorted(drops).forEach((value) => dropChips.append(makeChip(value, true)));
      }
    };

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
      renderCompressionSettings();
    };

    compressionPanel.querySelectorAll("[data-profile-option]").forEach((option) => {
      option.addEventListener("change", () => {
        assignProfile(option.dataset.profileOption, option.value, option.checked);
      });
    });
    compressionPanel.querySelectorAll("[data-profile-select-all]").forEach((toggle) => {
      toggle.addEventListener("change", () => {
        const profile = toggle.dataset.profileSelectAll;
        const selected = toggle.checked;
        const target = profileSets.get(profile);
        if (!target) return;
        compressionPanel
          .querySelectorAll(`[data-profile-option="${profile}"]`)
          .forEach((option) => {
            if (selected) {
              profileSets.forEach((values, name) => {
                if (name !== profile) values.delete(option.value);
              });
              drops.delete(option.value);
              target.add(option.value);
            } else {
              target.delete(option.value);
            }
          });
        renderCompressionSettings();
      });
    });

    const addDrop = () => {
      if (!dropInput) return true;
      const modality = dropInput.value.trim().toUpperCase();
      if (!modality) return true;
      if (!/^[A-Z0-9]{1,8}$/.test(modality)) {
        dropInput.setCustomValidity("Use um código DICOM com até 8 letras ou números.");
        dropInput.reportValidity();
        return false;
      }
      dropInput.setCustomValidity("");
      drops.add(modality);
      profileSets.forEach((values) => values.delete(modality));
      dropInput.value = "";
      renderCompressionSettings();
      return true;
    };
    dropInput?.addEventListener("input", () => dropInput.setCustomValidity(""));
    dropInput?.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === ",") {
        event.preventDefault();
        addDrop();
      } else if (event.key === "Backspace" && !dropInput.value && drops.size) {
        drops.delete(sorted(drops).at(-1));
        renderCompressionSettings();
      }
    });
    dropInput?.addEventListener("blur", addDrop);

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
    unitForm?.addEventListener("submit", (event) => {
      if (!addDrop()) event.preventDefault();
      renderCompressionSettings();
    });
    renderCompressionSettings();
  }

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
