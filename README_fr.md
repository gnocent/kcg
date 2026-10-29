*(English version: [README.md](README.md))*

# kcg

[![CI](https://github.com/gnocent/kcg/actions/workflows/ci.yml/badge.svg)](https://github.com/gnocent/kcg/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/gnocent/kcg?include_prereleases)](https://github.com/gnocent/kcg/releases)
[![License](https://img.shields.io/github/license/gnocent/kcg)](LICENSE)

Gérer les offsets des consumer groups Kafka sans hésiter sur le bon groupe : comparer les groupes côte à côte, aligner
l'un sur l'autre, rejouer depuis le début ou depuis un offset, et faire le ménage. Un binaire statique unique (Go,
franz-go), pour Kafka 2.6 ou plus récent.

## Démo

<p align="center"><img src="demokcg.gif" alt="Démo animée : kcg read montre trois groupes billing sur un topic, billing-v2 étant le plus avancé ; align recopie billing-v2 sur billing, d'abord en simulation puis avec --execute et une sauvegarde ; clean refuse car billing et billing-v2 sont désormais identiques, puis --keep billing-v2 supprime les deux autres ; un dernier read ne montre plus que billing-v2"></p>

> [!NOTE]
> **Comment ce projet est fait.** Je ne suis pas développeur : j'écris les spécifications et je vérifie le résultat ;
> le code est écrit par [Claude Code](https://claude.com/claude-code). Ce qui compense : des tests unitaires lancés avec
> le détecteur de concurrence, et une recette automatisée de 6 critères rejouée sur un banc Docker avec deux versions de
> Kafka (3.9 et 4.4, KRaft), dont un consumer group KIP-848. C'est une **première version publique (0.1.0)**, encore peu
> éprouvée : ce qui a été vérifié, et ce qui ne l'a pas été, figure dans la section [Sécurité](#sécurité).

## Pourquoi kcg

`kafka-consumer-groups.sh` travaille un groupe à la fois. Quand un consumer a été redéployé sous un nouveau nom de
groupe, ou que plusieurs groupes traînent sur le même topic, les questions portent sur les groupes *ensemble* : lequel
est en avance sur chaque partition, lequel garder, comment mettre les autres sur les mêmes offsets.

- **Un tableau pour tous les groupes.** `read` affiche une ligne par partition et une colonne par groupe, avec le
  début et la fin du log ; le groupe le plus avancé de chaque partition est signalé, comme les groupes actifs et les
  offsets sous le début du log.
- **Aligner en une commande.** `align` recopie les offsets d'un groupe, du minimum ou du maximum de plusieurs groupes,
  du début ou de la fin du log, ou d'un offset, sur un groupe ou sur tous.
- **Faire le ménage sans se tromper de groupe.** `clean` garde un groupe de référence par topic et refuse quand le choix
  est ambigu, en listant les partitions en conflit.
- **Rien d'écrit par mégarde.** Chaque commande d'écriture est une simulation sans `--execute`, refuse les groupes qui
  ont des membres actifs, exige `--force` pour reculer un offset et sauvegarde les offsets d'avant dans un CSV que
  `restore` remet en place.

## Installation

Télécharger l'archive de votre système dans les [releases](https://github.com/gnocent/kcg/releases) (Linux amd64,
macOS arm64, Windows amd64 ; sommes de contrôle dans `SHA256SUMS`), ou compiler avec Go 1.27.3 ou plus récent :

```bash
go install github.com/gnocent/kcg@latest
```

## Connexion

Comme pour les outils Kafka : `--bootstrap-server host:port[,host:port]` et, au besoin, `--command-config client.properties`.
Clés reconnues :

- `security.protocol` : PLAINTEXT, SSL, SASL_PLAINTEXT ou SASL_SSL ;
- `sasl.mechanism` (PLAIN, SCRAM-SHA-256 ou SCRAM-SHA-512) et `sasl.jaas.config` (`username`, `password`) ;
- `ssl.truststore.*` et `ssl.keystore.*` en PEM (fichier, ou en ligne avec `ssl.truststore.certificates`,
  `ssl.keystore.key` et `ssl.keystore.certificate.chain`) ou en PKCS12. JKS n'est pas pris en charge : le convertir en
  PKCS12 ;
- une valeur vide pour `ssl.endpoint.identification.algorithm` désactive la vérification du nom d'hôte (la chaîne de
  certificats reste vérifiée).

`--topic` et `--group` acceptent une liste séparée par des virgules et les jokers `*` (toute suite de caractères, `/`
compris) et `?` (un caractère).

## Commandes

| Commande | Effet |
|---|---|
| `read` | Une ligne par partition, une colonne par groupe. `*` signale le groupe le plus avancé, `!` un offset sous le début du log, `(active)` un groupe qui a des membres. |
| `align --from X --to Y` | Recopie des offsets. `X` vaut `oldest` ou `newest` (minimum ou maximum par partition des groupes filtrés), `earliest` ou `latest` (début ou fin du log), un offset (refusé s'il sort du log d'une partition), ou un groupe ; un nombre désigne toujours un offset, jamais un groupe. `Y` est un groupe, ou `all` pour tous les groupes filtrés. Une partition sans offset de référence est ignorée. |
| `delete [--partition 0,1]` | Supprime les offsets des groupes filtrés sur les topics filtrés, sur toutes les partitions sauf si `--partition` est donné. |
| `clean [--keep G]` | Ne garde qu'un groupe par topic et supprime les offsets des autres. Les groupes actifs sont toujours gardés, avec un avertissement si l'un d'eux est en retard. Si tous sont inactifs, kcg garde le plus avancé à condition qu'il le soit sur toutes les partitions ; sinon il refuse et liste les partitions en conflit, et `--keep` tranche. |
| `restore <fichier.csv>` | Remet les offsets d'une sauvegarde. |

Pour les écritures (`align`, `delete`, `clean`), `--topic` est obligatoire, `--group` aussi, sauf pour un `align` d'un
groupe nommé vers un autre. Écrire `'*'` pour tout viser. `kcg <commande> -h` liste les options d'une commande ;
`kcg -version` affiche la version.

## Sécurité des écritures

- Sans `--execute`, kcg affiche le plan et n'écrit rien.
- Un groupe qui a des membres actifs est refusé, qu'il soit classic ou KIP-848 ; Kafka rejette lui-même la modification
  des offsets d'un groupe classic actif.
- Reculer un offset avec `align` exige `--force`. `restore` en est dispensé, puisqu'il annule.
- Chaque `--execute` écrit d'abord les offsets d'avant dans `kcg-backup-<date>.csv`, dans le répertoire courant
  (`group,topic,partition,offset`, offset vide quand il n'y en avait pas). Pour annuler :
  `kcg restore <fichier> --execute`.
- Supprimer un offset est définitif : au redémarrage, le consumer repart selon son `auto.offset.reset`.

## Limites connues

- La sauvegarde ne conserve ni la metadata de commit ni le leader epoch : `restore` remet les offsets, pas la metadata
  (Kafka Streams y range le partition time).
- Un truststore PKCS12 doit marquer ses certificats comme de confiance, ce que fait `keytool`. Un `.p12` fabriqué avec
  `openssl` est refusé : utiliser alors un truststore PEM.
- Les noms de groupe sont écrits tels quels dans le CSV de sauvegarde. Un nom qui commence par `=`, `+`, `-` ou `@` peut
  être interprété comme une formule par un tableur.

## Développement

Sous Linux ou WSL : `make check` lance vet, les tests unitaires et le build. La recette (`make recette`) tourne sur le
[banc de test de kafkabeat](https://github.com/gnocent/kafkabeat/tree/main/bench) : Kafka 3.9 sur `localhost:19092` et
4.4 sur `localhost:19094`, adresses modifiables par `KCG_KAFKA39` et `KCG_KAFKA44`. `make demo` enregistre la démo
ci-dessus (nécessite [VHS](https://github.com/charmbracelet/vhs)).

## Sécurité

**Ce que kcg touche**

- **Kafka** : métadonnées des topics, offsets de début et de fin du log, liste des consumer groups et leurs offsets
  committés. Seules les commandes d'écriture lancées avec `--execute` écrivent : commits et suppressions d'offsets, pour
  les groupes et topics du plan affiché. kcg ne lit ni n'écrit jamais de messages.
- **Disque** : les fichiers de sauvegarde, dans le répertoire courant.
- **Identifiants** : lus uniquement dans le fichier `--command-config` (le garder en `0600`), jamais affichés. TLS 1.2
  au minimum.

**Ce qui a été vérifié, et ce qui ne l'a pas été**

- Tests unitaires avec le détecteur de concurrence, `go vet`, `staticcheck` et `govulncheck` ; la recette sur le banc.
  Il a été vérifié que Kafka refuse lui-même l'écriture d'offsets sur un groupe classic actif : le contrôle de kcg n'est
  pas la seule barrière.
- Le banc n'active aucune sécurité : SASL et TLS (PEM, PKCS12, mTLS) ne sont couverts que par des tests unitaires de la
  configuration, pas éprouvés contre un cluster sécurisé.
- **Aucun audit externe indépendant.** Pour signaler une vulnérabilité, ne pas ouvrir d'issue publique : utiliser le
  signalement privé de GitHub (onglet **Security** → **Report a vulnerability**), avec la version (`kcg -version`).
  Accusé de réception sous deux semaines environ au mieux, sans engagement de date de correctif ; seule la dernière
  version est maintenue.

## Licence

Ce projet est distribué sous **[GNU Affero General Public License v3.0](LICENSE)** (AGPLv3) : vous êtes libre de
l'utiliser, l'étudier, le modifier et le redistribuer, à condition que le code source (modifications comprises) reste
disponible sous les mêmes conditions — y compris quand le logiciel est exposé sur un réseau.

> **Note d'intention (sans valeur juridique)** : l'esprit de ce projet est de rester un outil communautaire, amélioré
> collectivement, et non un produit revendu en l'état. L'AGPLv3 n'interdit pas formellement l'usage commercial, mais
> c'est l'usage que son auteur espère en voir fait.

## Avertissement / limitation de responsabilité

kcg est publié **en l'état**, sans garantie d'aucune sorte, expresse ou implicite — notamment, sans s'y limiter, les
garanties de qualité marchande, d'adéquation à un usage particulier et d'absence de contrefaçon (voir les sections 15 à
17 de la [licence AGPLv3](LICENSE), qui font foi).

En particulier :

- Ce projet est développé et maintenu **sur le temps libre de son auteur**, sans engagement de disponibilité, de
  maintenance, de correctifs de sécurité ni d'évolution.
- L'auteur et les contributeurs **déclinent toute responsabilité** quant aux conséquences directes ou indirectes de
  l'usage de cet outil — notamment, sans s'y limiter, messages perdus ou retraités, perte de données ou interruption de
  service.
- kcg modifie les offsets des consumer groups avec les droits qu'on lui donne : **essayez-le d'abord sur un cluster de
  test** et lisez le plan avant d'ajouter `--execute`. L'utiliser contre un cluster de production relève de la seule
  responsabilité de la personne qui le fait.
