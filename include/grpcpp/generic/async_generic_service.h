// egrpc grpcpp shim — grpcpp/generic/async_generic_service.h (design §4.5).
// The generated header includes this unconditionally, but the only generic
// hooks it uses (Service::MarkMethodGeneric) live in impl/service_type.h;
// generic/async services themselves are server-side and permanently out of
// scope (design §2), so nothing is defined here.
#pragma once
