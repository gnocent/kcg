package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"software.sslmate.com/src/go-pkcs12"
)

// parseProperties lit un fichier .properties Java : séparateurs « = », « : »
// ou blanc, commentaires « # » et « ! », lignes continuées par « \ » (fréquent
// pour sasl.jaas.config) et échappements « \\ », « \t », « \n ».
func parseProperties(r io.Reader) (map[string]string, error) {
	props := map[string]string{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64*1024), 1024*1024)
	var logical strings.Builder
	for sc.Scan() {
		line := strings.TrimLeft(sc.Text(), " \t\f")
		if logical.Len() == 0 && (line == "" || line[0] == '#' || line[0] == '!') {
			continue
		}
		if continued(line) {
			logical.WriteString(line[:len(line)-1])
			continue
		}
		logical.WriteString(line)
		k, v := splitProperty(logical.String())
		props[k] = v
		logical.Reset()
	}
	if logical.Len() > 0 {
		k, v := splitProperty(logical.String())
		props[k] = v
	}
	return props, sc.Err()
}

// continued indique si la ligne finit par un nombre impair de « \ ».
func continued(line string) bool {
	n := 0
	for i := len(line) - 1; i >= 0 && line[i] == '\\'; i-- {
		n++
	}
	return n%2 == 1
}

func splitProperty(s string) (string, string) {
	var key strings.Builder
	i := 0
	for ; i < len(s); i++ {
		c := s[i]
		if c == '\\' && i+1 < len(s) {
			i++
			key.WriteByte(unescape(s[i]))
			continue
		}
		if c == '=' || c == ':' || c == ' ' || c == '\t' || c == '\f' {
			break
		}
		key.WriteByte(c)
	}
	rest := strings.TrimLeft(s[i:], " \t\f")
	if rest != "" && (rest[0] == '=' || rest[0] == ':') {
		rest = strings.TrimLeft(rest[1:], " \t\f")
	}
	var val strings.Builder
	for j := 0; j < len(rest); j++ {
		if rest[j] == '\\' && j+1 < len(rest) {
			j++
			val.WriteByte(unescape(rest[j]))
			continue
		}
		val.WriteByte(rest[j])
	}
	return key.String(), strings.TrimRight(val.String(), " \t\f")
}

func unescape(c byte) byte {
	switch c {
	case 't':
		return '\t'
	case 'n':
		return '\n'
	case 'r':
		return '\r'
	case 'f':
		return '\f'
	}
	return c
}

// clientOptions traduit les propriétés Kafka de connexion en options franz-go.
// Les clés inconnues sont ignorées, comme le font les outils Kafka pour les
// réglages qui ne concernent pas la connexion.
func clientOptions(props map[string]string) ([]kgo.Opt, error) {
	protocol := strings.ToUpper(props["security.protocol"])
	if protocol == "" {
		protocol = "PLAINTEXT"
	}
	var opts []kgo.Opt
	switch protocol {
	case "PLAINTEXT":
	case "SSL", "SASL_SSL", "SASL_PLAINTEXT":
	default:
		return nil, fmt.Errorf("security.protocol %q is not supported (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL)", protocol)
	}
	if strings.HasPrefix(protocol, "SASL_") {
		mech, err := saslMechanism(props)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.SASL(mech))
	}
	if protocol == "SSL" || protocol == "SASL_SSL" {
		cfg, err := tlsConfig(props)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(cfg))
	}
	return opts, nil
}

var jaasOption = regexp.MustCompile(`(\w+)\s*=\s*(?:"((?:[^"\\]|\\.)*)"|([^\s;"]+))`)

// saslMechanism lit sasl.mechanism et les identifiants de sasl.jaas.config.
// Kafka prend GSSAPI par défaut : le mécanisme doit donc être explicite.
func saslMechanism(props map[string]string) (sasl.Mechanism, error) {
	jaas := map[string]string{}
	for _, m := range jaasOption.FindAllStringSubmatch(props["sasl.jaas.config"], -1) {
		v := m[3]
		if v == "" {
			v = strings.NewReplacer(`\"`, `"`, `\\`, `\`).Replace(m[2])
		}
		jaas[m[1]] = v
	}
	user, pass := jaas["username"], jaas["password"]
	mech := strings.ToUpper(props["sasl.mechanism"])
	switch mech {
	case "PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512":
	case "":
		return nil, errors.New("sasl.mechanism is required with SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)")
	default:
		return nil, fmt.Errorf("sasl.mechanism %q is not supported (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", mech)
	}
	if user == "" {
		return nil, errors.New(`sasl.jaas.config must set username="..." and password="..."`)
	}
	switch mech {
	case "PLAIN":
		return plain.Auth{User: user, Pass: pass}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism(), nil
	default:
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism(), nil
	}
}

// tlsConfig construit la configuration TLS à partir du truststore et du
// keystore, en PEM (fichier ou valeur en ligne) ou en PKCS12.
func tlsConfig(props map[string]string) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}

	trust, err := storeData(props, "ssl.truststore", "certificates")
	if err != nil {
		return nil, err
	}
	if trust != nil {
		pool := x509.NewCertPool()
		switch trust.kind {
		case "PEM":
			if !pool.AppendCertsFromPEM(trust.data) {
				return nil, errors.New("ssl.truststore: no PEM certificate found")
			}
		case "PKCS12":
			certs, err := pkcs12.DecodeTrustStore(trust.data, props["ssl.truststore.password"])
			if err != nil {
				return nil, fmt.Errorf("ssl.truststore: %w", err)
			}
			for _, c := range certs {
				pool.AddCert(c)
			}
		}
		cfg.RootCAs = pool
	}

	key, err := storeData(props, "ssl.keystore", "key")
	if err != nil {
		return nil, err
	}
	if key != nil {
		cert, err := keyPair(props, key)
		if err != nil {
			return nil, err
		}
		cfg.Certificates = []tls.Certificate{cert}
	}

	// ssl.endpoint.identification.algorithm vide : la chaîne est vérifiée,
	// le nom d'hôte ne l'est pas (comportement Kafka).
	if alg, set := props["ssl.endpoint.identification.algorithm"]; set && alg == "" {
		roots := cfg.RootCAs
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("tls: no server certificate")
			}
			inter := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				inter.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: roots, Intermediates: inter})
			return err
		}
	}
	return cfg, nil
}

type store struct {
	kind string // PEM ou PKCS12
	data []byte
	// chain : chaîne de certificats en ligne (ssl.keystore.certificate.chain).
	chain []byte
}

// storeData charge un truststore ou un keystore depuis <prefix>.location ou,
// en PEM, depuis la valeur en ligne <prefix>.<inline>. Sans type déclaré,
// le format est deviné au contenu (Kafka prend JKS par défaut).
func storeData(props map[string]string, prefix, inline string) (*store, error) {
	kind := strings.ToUpper(props[prefix+".type"])
	if v := props[prefix+"."+inline]; v != "" {
		s := &store{kind: "PEM", data: normalizePEM(v)}
		if prefix == "ssl.keystore" {
			s.chain = normalizePEM(props["ssl.keystore.certificate.chain"])
		}
		return s, nil
	}
	loc := props[prefix+".location"]
	if loc == "" {
		return nil, nil
	}
	data, err := os.ReadFile(loc)
	if err != nil {
		return nil, fmt.Errorf("%s.location: %w", prefix, err)
	}
	if kind == "" {
		switch {
		case bytes.Contains(data, []byte("-----BEGIN")):
			kind = "PEM"
		case bytes.HasPrefix(data, []byte{0xFE, 0xED, 0xFE, 0xED}):
			kind = "JKS"
		default:
			kind = "PKCS12"
		}
	}
	switch kind {
	case "PEM", "PKCS12":
		return &store{kind: kind, data: data}, nil
	case "JKS":
		return nil, fmt.Errorf("%s: JKS is not supported, convert it to PKCS12 or PEM (keytool -importkeystore -deststoretype PKCS12)", prefix)
	}
	return nil, fmt.Errorf("%s.type %q is not supported (PEM, PKCS12)", prefix, kind)
}

var pemBlock = regexp.MustCompile(`(?s)-----BEGIN ([A-Z0-9 ]+)-----(.*?)-----END ([A-Z0-9 ]+)-----`)

// normalizePEM rétablit les sauts de ligne d'un PEM écrit en ligne dans un
// .properties, où les lignes continuées par « \ » sont jointes : Kafka
// l'accepte, pem.Decode non.
func normalizePEM(v string) []byte {
	if v == "" {
		return nil
	}
	var b bytes.Buffer
	for _, m := range pemBlock.FindAllStringSubmatch(v, -1) {
		body := strings.Join(strings.Fields(m[2]), "")
		fmt.Fprintf(&b, "-----BEGIN %s-----\n", m[1])
		for len(body) > 64 {
			b.WriteString(body[:64] + "\n")
			body = body[64:]
		}
		if body != "" {
			b.WriteString(body + "\n")
		}
		fmt.Fprintf(&b, "-----END %s-----\n", m[3])
	}
	return b.Bytes()
}

func keyPair(props map[string]string, s *store) (tls.Certificate, error) {
	if s.kind == "PKCS12" {
		key, cert, cas, err := pkcs12.DecodeChain(s.data, props["ssl.keystore.password"])
		if err != nil {
			return tls.Certificate{}, fmt.Errorf("ssl.keystore: %w", err)
		}
		c := tls.Certificate{PrivateKey: key, Leaf: cert, Certificate: [][]byte{cert.Raw}}
		for _, ca := range cas {
			c.Certificate = append(c.Certificate, ca.Raw)
		}
		return c, nil
	}
	certPEM := s.data
	if s.chain != nil {
		certPEM = s.chain
	}
	for rest := s.data; ; {
		var b *pem.Block
		b, rest = pem.Decode(rest)
		if b == nil {
			break
		}
		if b.Type == "ENCRYPTED PRIVATE KEY" || b.Headers["Proc-Type"] != "" {
			return tls.Certificate{}, errors.New("ssl.keystore: encrypted PEM keys are not supported, use an unencrypted key or PKCS12")
		}
	}
	cert, err := tls.X509KeyPair(certPEM, s.data)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("ssl.keystore: %w", err)
	}
	return cert, nil
}
