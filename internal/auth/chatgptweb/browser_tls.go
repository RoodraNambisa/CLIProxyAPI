package chatgptweb

import (
	"github.com/bogdanfinn/tls-client/profiles"
	tls "github.com/bogdanfinn/utls"
)

// chatGPTWebChrome146Profile preserves the browser identity and HTTP settings,
// but does not advertise trust-anchor negotiation through a generic extension.
// This is the explicitly selected compatibility variant. Certificate verification,
// cipher suites, supported versions and all other extensions are unchanged.
func chatGPTWebChrome146Profile() profiles.ClientProfile {
	base := profiles.Chrome_146
	id := base.GetClientHelloId()
	id.SpecFactory = func() (tls.ClientHelloSpec, error) {
		spec, err := base.GetClientHelloSpec()
		if err != nil {
			return spec, err
		}
		extensions := make([]tls.TLSExtension, 0, len(spec.Extensions))
		for _, extension := range spec.Extensions {
			if generic, ok := extension.(*tls.GenericExtension); ok && generic.Id == 0xca34 {
				continue
			}
			extensions = append(extensions, extension)
		}
		spec.Extensions = extensions
		return spec, nil
	}
	return profiles.NewClientProfile(
		id,
		base.GetSettings(),
		base.GetSettingsOrder(),
		base.GetPseudoHeaderOrder(),
		base.GetConnectionFlow(),
		base.GetPriorities(),
		base.GetHeaderPriority(),
		base.GetStreamID(),
		base.GetAllowHTTP(),
		base.GetHttp3Settings(),
		base.GetHttp3SettingsOrder(),
		base.GetHttp3PriorityParam(),
		base.GetHttp3PseudoHeaderOrder(),
		base.GetHttp3SendGreaseFrames(),
	)
}
