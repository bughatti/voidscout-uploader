# Code signing policy

Free code signing provided by [SignPath.io](https://about.signpath.io), certificate by [SignPath Foundation](https://signpath.org).

## What is signed

The Windows release binary, `voidscout-uploader-windows-amd64.exe`. It is built by
[GitHub Actions](.github/workflows/release.yml) from a tagged commit of this repository
(`v*.*.*` tags) and signed through SignPath before the GitHub Release is published. Nothing
built outside this workflow is signed. The macOS and Linux builds are not signed.

## Team roles

- Committers and reviewers: [bughatti](https://github.com/bughatti)
- Approvers: [bughatti](https://github.com/bughatti), the repository owner. Every release
  is started by pushing a version tag, and each signing request is approved by the owner.

All team members use multi-factor authentication on GitHub and SignPath.

## Privacy

The uploader only talks to two places:

1. **api.voidscout.io**, and only while uploads are turned on in the VoidScout addon
   ("Allow uploads" in its consent dialog; switching to local-only stops them):
   - fight summaries and player profiles the addon recorded in its saved data;
   - the World of Warcraft combat log, cut into one piece per boss fight (and per Mythic+
     trash section), so the fights can be scored;
   - with the addon's optional damage-sim sharing on, the same uploads are marked for the
     damage simulator, which stores players under anonymous IDs;
   - a deletion request, when the player chooses "Delete + go local" in the addon. This is
     sent even with uploads off, since it is the opt-out.

   Fights recorded by the separate VoidRaidTools Reader addon are uploaded under that
   addon's own consent.
2. **api.github.com / github.com**, to check this repository for a newer release on launch
   and download it. Start the uploader with `-no-update` to turn this off.

It sends nothing else and runs no other network connections. Full details:
[voidscout.io/privacy](https://voidscout.io/privacy).

## Contact

support@voidscout.io
