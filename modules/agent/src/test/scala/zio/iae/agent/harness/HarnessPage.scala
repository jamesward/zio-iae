package zio.iae.agent.harness

/** The harness UI: plain HTML and a little JavaScript, polling the harness for state. */
object HarnessPage:
  val html: String =
    """<!doctype html>
      |<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
      |<title>iae harness</title>
      |<style>
      |  :root { --bg:#fff; --fg:#1d1c1d; --muted:#616061; --line:#e2e2e2; --accent:#1264a3; --card:#f8f8f8; }
      |  @media (prefers-color-scheme: dark) { :root { --bg:#1a1d21; --fg:#d1d2d3; --muted:#9a9b9c; --line:#35373b; --accent:#4aa3df; --card:#222529; } }
      |  body { margin:0; font:14px/1.45 system-ui, sans-serif; background:var(--bg); color:var(--fg); }
      |  header { padding:12px 16px; border-bottom:1px solid var(--line); display:flex; gap:12px; align-items:center; flex-wrap:wrap; }
      |  main { display:grid; grid-template-columns: minmax(0,2fr) minmax(0,1fr); gap:16px; padding:16px; }
      |  @media (max-width: 800px) { main { grid-template-columns: 1fr; } }
      |  .msg { border:1px solid var(--line); border-radius:8px; padding:10px 12px; margin-bottom:10px; background:var(--card); }
      |  .who { font-weight:600; } .ts { color:var(--muted); font-size:12px; margin-left:6px; }
      |  pre { white-space:pre-wrap; margin:6px 0; font-size:12px; }
      |  button { border:1px solid var(--line); background:var(--bg); color:var(--fg); border-radius:6px; padding:3px 8px; cursor:pointer; }
      |  button:hover { border-color:var(--accent); } .reacted { border-color:var(--accent); }
      |  textarea, input, select { width:100%; box-sizing:border-box; background:var(--bg); color:var(--fg); border:1px solid var(--line); border-radius:6px; padding:6px; font:12px ui-monospace, monospace; }
      |  .reply { margin:6px 0 0 16px; padding-left:8px; border-left:2px solid var(--line); }
      |  h2 { font-size:15px; margin:0 0 8px; } select { width:auto; font:inherit; }
      |</style></head><body>
      |<header><strong>iae harness</strong> <span id="channel"></span>
      |  <label>You are <select id="user"></select></label>
      |  <span id="clock" title="The agent's clock"></span>
      |  <label>speed <input id="speed" type="number" min="1" max="10000" style="width:80px"></label>
      |  <button onclick="advance(60)">+1h</button> <button onclick="advance(1440)">+1d</button></header>
      |<main>
      |  <section><h2>Slack</h2><div id="messages"></div></section>
      |  <section><h2>GitHub</h2><div id="files"></div>
      |    <input id="commitMessage" placeholder="commit message" value="change infra">
      |    <p><button id="push">Commit and push</button></p>
      |    <h2>Commits</h2><pre id="commits"></pre>
      |    <h2>Faults</h2><p>Injected into the version in canary (a bad deploy) or the live one (an incident).</p><div id="faults"></div>
      |    <h2>Scenarios</h2><div id="run"></div><div id="scenarios"></div></section>
      |</main>
      |<script>
      |const post = (path, body) => fetch(path, {method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify(body)});
      |const esc = s => s.replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c]));
      |let filesShown = false, usersShown = false, scenariosShown = false;
      |async function refresh() {
      |  const s = await (await fetch('/ui/state')).json();
      |  document.getElementById('channel').textContent = '#' + s.channel;
      |  if (!usersShown) {
      |    const sel = document.getElementById('user');
      |    s.users.filter(u => !u.bot).forEach(u => sel.add(new Option(u.name + ' (' + u.id + ')', u.id)));
      |    usersShown = true;
      |  }
      |  const me = document.getElementById('user').value;
      |  const name = id => (s.users.find(u => u.id === id) || {name:id}).name;
      |  document.getElementById('messages').innerHTML = s.messages.slice().reverse().map(m => `
      |    <div class="msg"><span class="who">${esc(name(m.user))}</span><span class="ts">${m.ts}</span>
      |      <pre>${esc(m.text)}</pre>
      |      ${['white_check_mark','x'].map(r => {
      |          const who = (m.reactions[r] || []);
      |          return `<button class="${who.includes(me) ? 'reacted' : ''}" onclick="react('${m.ts}','${r}')">${r === 'x' ? '❌' : '✅'} ${who.map(name).join(', ')}</button>`;
      |        }).join(' ')}
      |      ${m.replies.map(r => `<div class="reply"><span class="who">${esc(name(r.user))}</span> ${esc(r.text)}</div>`).join('')}
      |      <div class="reply"><input placeholder="reply in thread" onkeydown="if(event.key==='Enter'){reply('${m.ts}', this.value); this.value='';}"></div>
      |    </div>`).join('') || '<p>No messages yet.</p>';
      |  if (!filesShown) {
      |    document.getElementById('files').innerHTML = Object.entries(s.files).map(([path, text]) =>
      |      `<p><strong>${esc(path)}</strong><textarea rows="10" data-path="${esc(path)}">${esc(text)}</textarea></p>`).join('');
      |    filesShown = true;
      |  }
      |  document.getElementById('commits').textContent = s.commits.join('\n');
      |  document.getElementById('clock').textContent = '🕑 ' + s.clock.now.replace('T', ' ').slice(0, 16) + ' UTC';
      |  const speed = document.getElementById('speed');
      |  if (document.activeElement !== speed) speed.value = s.clock.speed;
      |  const r = s.run;
      |  document.getElementById('run').innerHTML = r ? `<p><strong>${esc(r.name)}</strong>: ${r.status} — step ${Math.min(r.step, r.total)}/${r.total}: ${esc(r.current)}${r.error ? '<br>' + esc(r.error) : ''}</p>` : '';
      |  const faults = [['Errors','answers 5xx'],['Slow','answers slowly'],['FailingMetric','reports failing uploads']];
      |  document.getElementById('faults').innerHTML = s.envs.flatMap(env => ['Canary', 'Live'].map(target => `<p><strong>${esc(env)}</strong> ${target.toLowerCase()}: ` + faults.map(([f, label]) => {
      |      const on = s.faults.some(a => a.env === env && a.fault === f && a.target === target);
      |      return `<button class="${on ? 'reacted' : ''}" onclick="fault('${env}','${f}','${target}',${!on})">${on ? '⚠ ' : ''}${label}</button>`;
      |    }).join(' ') + '</p>')).join('');
      |  if (!scenariosShown) {
      |    document.getElementById('scenarios').innerHTML = s.scenarios.map(sc =>
      |      `<p><button onclick="runScenario('${sc.name}')">Run ${esc(sc.name)}</button> ${esc(sc.description)}</p>`).join('');
      |    scenariosShown = true;
      |  }
      |}
      |function fault(env, fault, target, on) { post('/ui/fault', {env, fault, target, on}).then(refresh); }
      |function runScenario(name) { post('/ui/scenario', {name}).then(refresh); }
      |function advance(minutes) { post('/ui/clock/advance', {minutes}).then(refresh); }
      |document.getElementById('speed').onchange = e => post('/ui/clock/speed', {speed: Number(e.target.value)}).then(refresh);
      |function react(ts, reaction) { post('/ui/react', {ts, user: document.getElementById('user').value, reaction}).then(refresh); }
      |function reply(thread, text) { post('/ui/reply', {thread, user: document.getElementById('user').value, text}).then(refresh); }
      |document.getElementById('push').onclick = () => {
      |  const files = {};
      |  document.querySelectorAll('textarea[data-path]').forEach(t => files[t.dataset.path] = t.value);
      |  post('/ui/push', {files, message: document.getElementById('commitMessage').value}).then(refresh);
      |};
      |document.getElementById('user').onchange = refresh;
      |refresh(); setInterval(refresh, 1500);
      |</script></body></html>""".stripMargin
