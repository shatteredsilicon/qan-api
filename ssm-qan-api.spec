%define debug_package %{nil}

# it is impossible to pass ldflags to revel, so disable check
%undefine _missing_build_ids_terminate_build

%global provider        github
%global provider_tld	com
%global project         shatteredsilicon
%global repo            qan-api
%global provider_prefix	%{provider}.%{provider_tld}/%{project}/%{repo}

Name:		ssm-qan-api
Version:	%{_version}
Release:	1%{?dist}
Summary:	Query Analytics API for SSM

License:	AGPLv3
URL:		https://%{provider_prefix}
Source0:	%{name}-%{version}.tar.gz

BuildRequires:	golang
Requires:	perl

%if 0%{?fedora} || 0%{?rhel} == 7
BuildRequires: systemd
Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd
%endif

%description
Shattered Silicon Query Analytics (QAN) API is part of Shattered Silicon Monitoring and Management.
See the SSM docs for more information.


%prep
%setup -T -c -n %{name}
%setup -q -c -a 0 -n %{name}
mkdir -p ${HOME}/go/src/%{provider}.%{provider_tld}/%{project}
mv %{name} ${HOME}/go/src/%{provider_prefix}
cp -r ${HOME}/go/src/%{provider_prefix}/vendor/github.com/revel ${HOME}/go/src/github.com/revel


%build
mkdir -p bin release
cp -r $(go env GOROOT) ${HOME}/goroot
export GOROOT=${HOME}/goroot
export GOPATH=${HOME}/go
export APP_VERSION="%{version}"
GO111MODULE=off go build -o ./revel ${GOPATH}/src/%{provider_prefix}/vendor/github.com/revel/cmd/revel
GO111MODULE=off ./revel build %{provider_prefix} release prod
rm -rf release/src/github.com/shatteredsilicon/qan-api
mkdir -p ./src
mv ${HOME}/go/src/%{provider_prefix}/* ./src/


%install
install -d -p %{buildroot}%{_sbindir}
mv ./release/%{repo} %{buildroot}%{_sbindir}/%{name}
install -d -p %{buildroot}%{_datadir}
cp -rpa ./release %{buildroot}%{_datadir}/%{name}
install -d -p %{buildroot}%{_datadir}/%{name}/src/%{provider_prefix}/app/views
install -d -p %{buildroot}%{_datadir}/%{name}/src/%{provider_prefix}/service/query
cp -rpa ./src/schema    %{buildroot}%{_datadir}/%{name}/schema
cp -rpa ./src/conf      %{buildroot}%{_datadir}/%{name}/src/%{provider_prefix}/conf
cp -rpa ./src/service/query/mini.pl %{buildroot}%{_datadir}/%{name}/src/%{provider_prefix}/service/query/mini.pl

install -d -p %{buildroot}%{_sysconfdir}
cp -rpa ./src/conf/prod.conf %{buildroot}%{_sysconfdir}/ssm-qan-api.conf

install -d %{buildroot}/usr/lib/systemd/system
install -p -m 0644 ./src/%{name}.service %{buildroot}/usr/lib/systemd/system/%{name}.service


%post
%if 0%{?fedora} || 0%{?rhel} == 7
%systemd_post %{name}.service
%else
#/sbin/chkconfig --add %{name}
%endif

%preun
%if 0%{?fedora} || 0%{?rhel} == 7
%systemd_preun %{name}.service
%else
if [ $1 = 0 ]; then
    #service %{name} stop >/dev/null 2>&1 ||:
    #/sbin/chkconfig --del %{name}
fi
%endif

%postun
%if 0%{?fedora} || 0%{?rhel} == 7
%systemd_postun %{name}.service
%else
if [ "$1" -ge "1" ]; then
    #service %{name} condrestart > /dev/null 2>&1 ||:
fi
%endif


%files
%license src/LICENSE
%doc src/README.md src/CHANGELOG.md
%attr(0755, root, root) %{_sbindir}/%{name}
%{_datadir}/%{name}
/usr/lib/systemd/system/%{name}.service
%config %{_sysconfdir}/ssm-qan-api.conf


%changelog
* Mon Mar  6 2017 Mykola Marzhan <mykola.marzhan@percona.com> - 1.7.0-3
- don't install obsolete scripts

* Mon Mar  6 2017 Mykola Marzhan <mykola.marzhan@percona.com> - 1.1.1-2
- mark percona-qan-api.conf as %config

* Thu Feb  2 2017 Mykola Marzhan <mykola.marzhan@percona.com> - 1.1.0-1
- add build_timestamp to Release value
- use deps from vendor dir

* Fri Dec 16 2016 Mykola Marzhan <mykola.marzhan@percona.com> - 1.0.7-1
- init version
