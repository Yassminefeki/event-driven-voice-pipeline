# ✅ DataBot — Liste complète des modifications à faire

Basé sur `BotTelegram_code_complet.md` (consolidation du 3 août 2026). Classé par priorité : 🔴 critique (sécurité/prod), 🟠 important (fiabilité), 🟡 structurel (organisation repo), 🟢 amélioration continue.

---

## 🔴 1. Sécurité — À FAIRE AVANT TOUT PUSH PUBLIC

- [ ] **Retirer les identifiants en clair** `admin / admin12345` (MinIO) du code/doc → déplacer vers variables d'environnement (`.env`, jamais commité).
- [ ] **Purger l'historique Git** si ces credentials ont déjà été commités (`git filter-repo` ou `bfg`), puis les régénérer (rotation obligatoire — un secret déjà poussé est compromis même après suppression).
- [ ] **Activer `xpack.security.enabled=true`** sur Elasticsearch (actuellement désactivé) + créer un utilisateur applicatif dédié.
- [ ] **Activer l'authentification Kafka (SASL/SSL)** — les brokers sont actuellement en Plaintext, sans auth (`kafka1:9092`, etc.).
- [ ] **Sécuriser la Kafka Connect REST API** (`:8083`) — actuellement sans authentification, accessible à quiconque a accès réseau.
- [ ] **Vérifier l'exposition de l'API Whisper** (`10.110.150.77`) — sans authentification, potentiellement accessible en dehors du réseau attendu.
- [ ] Créer `.env.example` avec toutes les clés nécessaires **sans valeurs réelles**.
- [ ] Ajouter `.gitignore` couvrant : `.env`, `__pycache__/`, `*.log`, `venv/`, `*.sqlite3` (pour `object_name_store`).

---

## 🟠 2. Fiabilité & Résilience (code applicatif)

### ASR Worker (`whisper_worker.py`)
- [ ] Confirmer que `enable_auto_commit=False` est bien appliqué partout (pas seulement documenté).
- [ ] Vérifier que `_compute_safe_commit_offsets` gère le cas d'un **lot entièrement vide** ou d'un **lot 100% en échec** (edge case pas mentionné dans la doc — à tester).
- [ ] Vérifier que `ASR_WORKER_CONCURRENCY` est bien lu depuis l'environnement (pas en dur dans le code).
- [ ] Documenter/tester le comportement du **Single Long Timeout (900s)** : que se passe-t-il si le worker crash pendant ce timeout ? Le message est-il rejoué proprement au redémarrage ?

### Dead-Letter Queue
- [ ] **Implémenter le consumer DLQ manquant** (mentionné en roadmap §7 mais pas encore fait) : alerte Kibana OU consumer dédié sur `audio.uploaded.dlq`.
- [ ] Ajouter un mécanisme de **rejeu automatique** des messages DLQ après correction du service ASR (actuellement manuel/absent).
- [ ] Ajouter des **métriques de volumétrie DLQ** (compteur, âge du message le plus ancien) exposées pour supervision.

### Kafka Connect
- [ ] **Mettre à jour le connecteur Elasticsearch Sink vers v15.1.0+** (actuellement vulnérable au `NullPointerException` avec ES 8.15.0 sur le v14 — corrigé en v15.1.0+ selon §6.10).
- [ ] Vérifier qu'aucune **instance fantôme de Kafka Connect** ne persiste sur `kafka1` (incident §6.7) — auditer les plugins installés sur `kafka1` vs `kafkaconnect`.
- [ ] Revalider la politique `cleanup.policy=compact` sur le topic interne `connect-configs` (correctif §6.5).

### Logs & disque
- [ ] **Appliquer la rétention des logs** (roadmap §7, non fait) : `MaxBackupIndex=3` dans `/opt/kafka/config/log4j.properties` sur **les 3 brokers** (`kafka1`, `kafka2`, `kafka3`).
- [ ] Ajouter un monitoring d'espace disque (alerte à 80%) pour éviter la récidive de l'incident de saturation (§6.5).

---

## 🟡 3. Réorganisation du repo (structurel)

- [ ] Créer l'arborescence cible :
  ```
  databot/
  ├── bot/
  ├── asr-worker/
  ├── s3-publisher/
  ├── kafka/{kraft,connect,topics}/
  ├── elk/{elasticsearch,kibana}/
  ├── stresstest/
  ├── docs/
  └── tests/
  ```
- [ ] Déplacer le code actuel dans les bons dossiers selon sa responsabilité (bot Telegram / worker ASR / publisher S3).
- [ ] Extraire les configs Kafka Connect (`minio-s3-sink.json`, `elasticsearch-sink.json`) dans `kafka/connect/` — actuellement probablement en dur dans des scripts ou en config manuelle via `curl`.
- [ ] Créer `kafka/topics/create-topics.sh` pour rendre la création des 4 topics (`audio.uploaded`, `audio.transcribed`, `transcription.corrected`, `audio.uploaded.dlq`) reproductible et versionnée.
- [ ] Déplacer la documentation narrative (historique incidents §6, architecture détaillée) dans `docs/incidents.md` et `docs/architecture.md` — sortir ça du README pour ne pas l'alourdir.
- [ ] Ajouter `docs/runbook.md` : procédures d'exploitation (comment relancer un worker, comment vérifier la DLQ, comment ajouter un broker).

---

## 🟡 4. Tests & qualité

- [ ] Ajouter des tests unitaires pour :
  - `_compute_safe_commit_offsets` (cas limites : lot vide, tous en échec, partiellement en échec).
  - Le routage DLQ (payload Base64 invalide, JSON corrompu, clé manquante).
  - `object_name_store` (résolution `message_id` → `object_name`, cas non trouvé).
- [ ] Ajouter un test d'intégration qui exécute `stresstest.py mock-whisper` + `stresstest.py run` et vérifie automatiquement la formule Zero Loss :
  ```
  Messages Envoyés = Messages Indexés (ES) + Messages en DLQ
  ```
- [ ] Intégrer un lint (`ruff` ou `flake8`) et le lancer en pré-commit ou CI.

---

## 🟢 5. CI/CD & process (amélioration continue)

- [ ] Ajouter un workflow GitHub Actions : lint + tests unitaires à chaque PR.
- [ ] Ajouter un `CHANGELOG.md` si le rythme de mise à jour s'accélère (au-delà de l'historique incidents actuel).
- [ ] Mettre en place le tagging de version (`git tag vX.Y.Z`) à chaque déploiement stable en production.
- [ ] Exécuter systématiquement `stresstest.py run --count 500 --rate 20` avant chaque mise en production (déjà recommandé §7, à formaliser en étape obligatoire de la checklist de release).

---

## 📋 Résumé — Ordre d'exécution recommandé

1. **Sécurité** (section 1) — bloquant avant tout partage/déploiement.
2. **DLQ monitoring + rejeu automatique** (section 2) — c'est le point le plus critique resté ouvert dans ta propre roadmap.
3. **Mise à jour connecteur ES Sink v15.1.0+** — corrige un bug déjà identifié.
4. **Rétention des logs** — évite la récidive d'un incident déjà survenu.
5. **Réorganisation du repo** (section 3) — une fois le code stabilisé, pour repartir sur une base propre.
6. **Tests + CI** (sections 4-5) — pour sécuriser les évolutions futures.

---

## ❓ Ce qu'il me manque pour aller plus loin

Je n'ai que ta **documentation**, pas le code source réel (`whisper_worker.py`, `main.py`, etc.). Si tu uploades les fichiers `.py` actuels, je peux :
- vérifier lesquels de ces points sont déjà faits vs manquants,
- te donner les diffs exacts (pas juste la liste),
- réorganiser physiquement les fichiers dans la nouvelle arborescence.
