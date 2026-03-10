/*
** CNRS LPP PROJECT, 2025
** Cache
** File description:
** Type aliases for common Store configurations
*/

#pragma once

#include "store.hpp"

using Cache = _Store<DiskStorage, WithExpiration, WithEviction, WithTags, WithStats>;
using Index = _Store<DiskStorage>;
