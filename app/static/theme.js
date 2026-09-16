(() => {
  "use strict";

  const savedTheme = localStorage.getItem("rm-theme");
  const systemTheme = window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
  const theme = savedTheme === "dark" || savedTheme === "light" ? savedTheme : systemTheme;

  document.documentElement.dataset.theme = theme;
  document
    .querySelector('meta[name="theme-color"]')
    ?.setAttribute("content", theme === "dark" ? "#0b1725" : "#071b33");
})();
