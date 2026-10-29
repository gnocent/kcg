package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"software.sslmate.com/src/go-pkcs12"
)

func TestParseProperties(t *testing.T) {
	in := `# commentaire
! autre commentaire
security.protocol=SASL_SSL
sasl.mechanism : SCRAM-SHA-512
ssl.truststore.location C:\\certs\\ca.pem
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="alice" \
    password="p@ss\\"w";
empty=
`
	props, err := parseProperties(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"security.protocol":       "SASL_SSL",
		"sasl.mechanism":          "SCRAM-SHA-512",
		"ssl.truststore.location": `C:\certs\ca.pem`,
		"sasl.jaas.config":        `org.apache.kafka.common.security.scram.ScramLoginModule required username="alice" password="p@ss\"w";`,
		"empty":                   "",
	}
	for k, v := range want {
		if props[k] != v {
			t.Errorf("%s = %q, attendu %q", k, props[k], v)
		}
	}
	if len(props) != len(want) {
		t.Errorf("propriétés inattendues : %v", props)
	}
}

func TestSASL(t *testing.T) {
	for _, mech := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		m, err := saslMechanism(map[string]string{
			"sasl.mechanism":   mech,
			"sasl.jaas.config": `x required username="bob" password="secret";`,
		})
		if err != nil {
			t.Fatalf("%s : %v", mech, err)
		}
		if m.Name() != mech {
			t.Errorf("mécanisme %s, attendu %s", m.Name(), mech)
		}
	}
	jaas := `x required username=bob password="a \"b\"";`
	if _, err := saslMechanism(map[string]string{"sasl.mechanism": "PLAIN", "sasl.jaas.config": jaas}); err != nil {
		t.Errorf("valeur non citée : %v", err)
	}
	for _, props := range []map[string]string{
		{"sasl.jaas.config": `x required username="bob";`},
		{"sasl.mechanism": "GSSAPI", "sasl.jaas.config": `x required username="bob";`},
		{"sasl.mechanism": "PLAIN"},
	} {
		if _, err := saslMechanism(props); err == nil {
			t.Errorf("erreur attendue pour %v", props)
		}
	}
	if _, err := clientOptions(map[string]string{"security.protocol": "SSL_FOO"}); err == nil {
		t.Error("protocole inconnu accepté")
	}
}

// selfSigned fabrique une clé et un certificat auto-signé de test.
func selfSigned(t *testing.T) (*ecdsa.PrivateKey, *x509.Certificate, []byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kcg-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, _ := x509.ParseCertificate(der)
	kder, _ := x509.MarshalPKCS8PrivateKey(key)
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kder})
	return key, cert, certPEM, keyPEM
}

func TestTLSPEM(t *testing.T) {
	_, _, certPEM, keyPEM := selfSigned(t)
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	ks := filepath.Join(dir, "client.pem")
	os.WriteFile(ca, certPEM, 0o600)
	os.WriteFile(ks, append(append([]byte{}, certPEM...), keyPEM...), 0o600)

	// Types déclarés, puis devinés au contenu.
	for _, props := range []map[string]string{
		{"ssl.truststore.type": "PEM", "ssl.truststore.location": ca, "ssl.keystore.type": "PEM", "ssl.keystore.location": ks},
		{"ssl.truststore.location": ca, "ssl.keystore.location": ks},
		{"ssl.truststore.certificates": string(certPEM), "ssl.keystore.certificate.chain": string(certPEM), "ssl.keystore.key": string(keyPEM)},
	} {
		cfg, err := tlsConfig(props)
		if err != nil {
			t.Fatalf("%v : %v", props, err)
		}
		if cfg.RootCAs == nil || len(cfg.Certificates) != 1 || cfg.InsecureSkipVerify {
			t.Errorf("configuration TLS inattendue pour %v", props)
		}
	}

	cfg, err := tlsConfig(map[string]string{"ssl.truststore.location": ca, "ssl.endpoint.identification.algorithm": ""})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.InsecureSkipVerify || cfg.VerifyConnection == nil {
		t.Error("vérification de nom d'hôte non désactivée")
	}

	enc := filepath.Join(dir, "enc.pem")
	os.WriteFile(enc, append(append([]byte{}, certPEM...), pem.EncodeToMemory(&pem.Block{Type: "ENCRYPTED PRIVATE KEY", Bytes: []byte{1}})...), 0o600)
	if _, err := tlsConfig(map[string]string{"ssl.keystore.location": enc}); err == nil || !strings.Contains(err.Error(), "encrypted") {
		t.Errorf("clé chiffrée : %v", err)
	}
}

func TestTLSPKCS12AndJKS(t *testing.T) {
	key, cert, _, _ := selfSigned(t)
	dir := t.TempDir()
	ks, err := pkcs12.Modern.Encode(key, cert, nil, "kspass")
	if err != nil {
		t.Fatal(err)
	}
	ts, err := pkcs12.Modern.EncodeTrustStore([]*x509.Certificate{cert}, "tspass")
	if err != nil {
		t.Fatal(err)
	}
	ksPath, tsPath := filepath.Join(dir, "ks.p12"), filepath.Join(dir, "ts.p12")
	os.WriteFile(ksPath, ks, 0o600)
	os.WriteFile(tsPath, ts, 0o600)

	cfg, err := tlsConfig(map[string]string{
		"ssl.truststore.type": "PKCS12", "ssl.truststore.location": tsPath, "ssl.truststore.password": "tspass",
		"ssl.keystore.location": ksPath, "ssl.keystore.password": "kspass",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RootCAs == nil || len(cfg.Certificates) != 1 || cfg.Certificates[0].Leaf.Subject.CommonName != "kcg-test" {
		t.Error("PKCS12 mal chargé")
	}
	if _, err := tlsConfig(map[string]string{"ssl.keystore.location": ksPath, "ssl.keystore.password": "faux"}); err == nil {
		t.Error("mauvais mot de passe accepté")
	}

	jks := filepath.Join(dir, "ts.jks")
	os.WriteFile(jks, []byte{0xFE, 0xED, 0xFE, 0xED, 0, 0, 0, 2}, 0o600)
	for _, props := range []map[string]string{
		{"ssl.truststore.location": jks},
		{"ssl.truststore.type": "JKS", "ssl.truststore.location": tsPath},
	} {
		if _, err := tlsConfig(props); err == nil || !strings.Contains(err.Error(), "JKS") {
			t.Errorf("JKS : %v", err)
		}
	}
}

func TestFilter(t *testing.T) {
	f, err := parseFilter("orders-*, payments,g?")
	if err != nil {
		t.Fatal(err)
	}
	for name, want := range map[string]bool{"orders-eu": true, "orders-eu/x": true, "payments": true, "g1": true, "g12": false, "orders": false, "payments-x": false} {
		if f.match(name) != want {
			t.Errorf("match(%q) = %v", name, !want)
		}
	}
	// « * » couvre « / », légal dans un nom de groupe ; « [ » et « . » sont littéraux.
	all, _ := parseFilter("*")
	lit, _ := parseFilter("a[1].b")
	if !all.match("team/app") || !lit.match("a[1].b") || lit.match("a1xb") {
		t.Error("jokers mal interprétés")
	}
	for _, bad := range []string{"", " , "} {
		if _, err := parseFilter(bad); err == nil {
			t.Errorf("filtre %q accepté", bad)
		}
	}
}

func TestInlinePEMFromProperties(t *testing.T) {
	_, _, certPEM, keyPEM := selfSigned(t)
	// Format Kafka : PEM en ligne, lignes continuées par « \ » et jointes.
	inline := func(p []byte) string {
		return strings.ReplaceAll(strings.TrimSpace(string(p)), "\n", " \\\n  ")
	}
	in := "security.protocol=SSL\n" +
		"ssl.truststore.type=PEM\nssl.truststore.certificates=" + inline(certPEM) + "\n" +
		"ssl.keystore.type=PEM\nssl.keystore.certificate.chain=" + inline(certPEM) + "\n" +
		"ssl.keystore.key=" + inline(keyPEM) + "\n"
	props, err := parseProperties(strings.NewReader(in))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(props["ssl.truststore.certificates"], "\n") {
		t.Fatal("les lignes continuées devraient être jointes")
	}
	cfg, err := tlsConfig(props)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.RootCAs == nil || len(cfg.Certificates) != 1 {
		t.Error("PEM en ligne mal chargé")
	}
}
