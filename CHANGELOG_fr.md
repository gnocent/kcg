*(English version: [CHANGELOG.md](CHANGELOG.md))*

# Journal des modifications

Une entrée par version publiée, la plus récente en premier ; « Non publié » rassemble ce qui attend le prochain tag.

## v0.1.0 (2026-10-29)

Première version publique.

- `read` : une ligne par partition, une colonne par groupe, avec le début et la fin du log ; groupe le plus avancé,
  groupes actifs et offsets sous le début du log signalés.
- `align --from oldest|newest|earliest|latest|<offset>|<groupe> --to <groupe>|all`.
- `delete` (groupes × topics × partitions) et `clean` (un groupe de référence gardé par topic, refus avec les
  partitions en conflit en cas d'ambiguïté, `--keep` pour trancher).
- Simulation par défaut, `--execute` pour écrire, `--force` pour reculer un offset, groupes avec membres actifs refusés
  (classic et KIP-848), sauvegarde CSV avant chaque écriture et `restore`.
- Connexion comme les outils Kafka : `--bootstrap-server`, `--command-config` (SASL PLAIN/SCRAM, TLS en PEM ou PKCS12).
- Testé sur Kafka 3.9 et 4.4.
