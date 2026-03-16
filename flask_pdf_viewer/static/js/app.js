(function(){
  // Accent palette (close to corporate / modern)
  const palettes = [
    ['#2563eb','#0ea5e9'], // blue -> sky
    ['#7c3aed','#22c55e'], // purple -> green
    ['#0f766e','#22c55e'], // teal -> green
    ['#dc2626','#f97316'], // red -> orange
    ['#111827','#ef4444']  // slate -> red
  ];

  // Stable but "dynamic": changes per day + hour
  const now = new Date();
  const idx = (now.getDate() + now.getHours()) % palettes.length;
  const [a1,a2] = palettes[idx];
  const root = document.documentElement;
  root.style.setProperty('--accent', a1);
  root.style.setProperty('--accent2', a2);
})();
