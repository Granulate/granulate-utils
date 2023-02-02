from typing import Dict, List, Union

import pytest

from granulate_utils.java import JvmFlag, parse_jvm_flags


@pytest.mark.parametrize(
    "jvm_flags_string,expected_jvm_flags_list",
    [
        (
            """
    uintx NonNMethodCodeHeapSize                   = 7594288                                {pd product} {ergonomic}
    uintx NonProfiledCodeHeapSize                  = 122031976                              {pd product} {ergonomic}
     intx NumberOfLoopInstrToAlign                 = 4                                      {C2 product} {management}
     intx ObjectAlignmentInBytes                   = 8                                    {lp64_product} {internal}
   size_t OldPLABSize                              = 1024                                 {ARCH product} {default}
    uintx OldPLABWeight                            = 50                                        {product} {environment}
   size_t OldSize                                  = 5452592                                   {product} {default}
     bool OmitStackTraceInFastThrow                = true                                      {product} {attach}
ccstrlist OnError                                  = cat hs_err_pid%p.log
          OnError                                 += ps -ef                                    {product} {command line}
ccstrlist OnOutOfMemoryError                       =                                     {C2 pd product} {default}
     intx OnStackReplacePercentage                 = 140                                    {pd product} {config file}\n"""  # noqa: E501
            """     bool OptimizeFill                             = true                                   {C2 product} {command line, ergonomic}\n"""  # noqa: E501
            """     bool OptimizePtrCompare                       = true                                   {C2 product} {default}""",  # noqa: E501
            [
                JvmFlag(
                    name="NonNMethodCodeHeapSize",
                    type="uintx",
                    value="7594288",
                    origin="ergonomic",
                    kind=["pd", "product"],
                ),
                JvmFlag(
                    name="NonProfiledCodeHeapSize",
                    type="uintx",
                    value="122031976",
                    origin="ergonomic",
                    kind=["pd", "product"],
                ),
                JvmFlag(
                    name="NumberOfLoopInstrToAlign", type="intx", value="4", origin="management", kind=["C2", "product"]
                ),
                JvmFlag(
                    name="ObjectAlignmentInBytes", type="intx", value="8", origin="internal", kind=["lp64_product"]
                ),
                JvmFlag(name="OldPLABSize", type="size_t", value="1024", origin="default", kind=["ARCH", "product"]),
                JvmFlag(name="OldPLABWeight", type="uintx", value="50", origin="environment", kind=["product"]),
                JvmFlag(name="OldSize", type="size_t", value="5452592", origin="default", kind=["product"]),
                JvmFlag(name="OmitStackTraceInFastThrow", type="bool", value="true", origin="attach", kind=["product"]),
                JvmFlag(
                    name="OnStackReplacePercentage",
                    type="intx",
                    value="140",
                    origin="config file",
                    kind=["pd", "product"],
                ),
                JvmFlag(
                    name="OptimizeFill",
                    type="bool",
                    value="true",
                    origin="command line, ergonomic",
                    kind=["C2", "product"],
                ),
                JvmFlag(name="OptimizePtrCompare", type="bool", value="true", origin="default", kind=["C2", "product"]),
            ],
        ),
        (
            """
    uintx NewSize                                  := 357564416                           {product}
    uintx NewSizeThreadIncrease                     = 5320                                {pd product}
     intx NmethodSweepActivity                      = 10                                  {product}
    uintx OldPLABWeight                             = 50                                  {lp64_product}
    uintx OldSize                                  := 716177408                           {product}
     bool OmitStackTraceInFastThrow                 = true                                {product}
ccstrlist OnError                                  := cat hs_err_pid%p.log
      OnError                             += ps -ef                              {product}
ccstrlist OnOutOfMemoryError                        =                                     {product}
     intx OnStackReplacePercentage                  = 140                                 {pd product}
     bool OptoBundling                              = false                               {C2 pd product}
""",
            [
                JvmFlag(name="NewSize", type="uintx", value="357564416", origin="non-default", kind=["product"]),
                JvmFlag(
                    name="NewSizeThreadIncrease", type="uintx", value="5320", origin="default", kind=["pd", "product"]
                ),
                JvmFlag(name="NmethodSweepActivity", type="intx", value="10", origin="default", kind=["product"]),
                JvmFlag(name="OldPLABWeight", type="uintx", value="50", origin="default", kind=["lp64_product"]),
                JvmFlag(name="OldSize", type="uintx", value="716177408", origin="non-default", kind=["product"]),
                JvmFlag(
                    name="OmitStackTraceInFastThrow", type="bool", value="true", origin="default", kind=["product"]
                ),
                JvmFlag(
                    name="OnStackReplacePercentage", type="intx", value="140", origin="default", kind=["pd", "product"]
                ),
                JvmFlag(
                    name="OptoBundling", type="bool", value="false", origin="default", kind=["C2", "pd", "product"]
                ),
            ],
        ),
    ],
)
def test_parse_jvm_flags(jvm_flags_string: str, expected_jvm_flags_list: List[JvmFlag]) -> None:
    assert parse_jvm_flags(jvm_flags_string) == expected_jvm_flags_list


@pytest.mark.xfail
@pytest.mark.parametrize(
    "jvm_flags_string,expected_jvm_flags_list",
    [
        (
            """
ccstrlist OnError                                  = echo a
          OnError                                 += echo b                                    {product} {command line}
ccstrlist OnOutOfMemoryError                       =                                     {C2 pd product} {default}
""",
            [
                JvmFlag(
                    name="OnError", type="ccstrlist", value=["echo a", "echo b"], origin="command line", kind=["product"]  # type: ignore # noqa: E501
                ),
                JvmFlag(
                    name="OnOutOfMemoryError",
                    type="ccstrlist",
                    value="",
                    origin="default",
                    kind=["C2", "pd", "product"],
                ),
            ],
        ),
    ],
)
def test_parse_not_supported_jvm_flags(jvm_flags_string: str, expected_jvm_flags_list: List[JvmFlag]) -> None:
    assert parse_jvm_flags(jvm_flags_string) != expected_jvm_flags_list


@pytest.mark.parametrize(
    "jvm_flag,expected_jvm_flag_serialized",
    [
        (
            JvmFlag(
                name="NonNMethodCodeHeapSize", type="uintx", value="7594288", origin="ergonomic", kind=["pd", "product"]
            ),
            {
                "kind": ["pd", "product"],
                "name": "NonNMethodCodeHeapSize",
                "origin": "ergonomic",
                "type": "uintx",
                "value": "7594288",
            },
        ),
    ],
)
def test_jvm_flag_serialization(
    jvm_flag: JvmFlag, expected_jvm_flag_serialized: Dict[str, Dict[str, Union[str, List[str]]]]
) -> None:
    jvm_flag_serialized = jvm_flag.to_dict()
    assert jvm_flag_serialized == expected_jvm_flag_serialized
    assert JvmFlag.from_dict(jvm_flag_serialized) == jvm_flag
