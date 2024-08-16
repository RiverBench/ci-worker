package io.github.riverbench.ci_worker
package util

object SemVer:
  object Version:
    private val versionPattern = """^(\d+)\.(\d+)\.(\d+)(.*)""".r

    def apply(s: String): Version = tryParse(s).getOrElse(
      throw new IllegalArgumentException(s"Invalid version string: $s")
    )

    def tryParse(s: String): Option[Version] = s match
      case "dev" => Some(DevVersion)
      case versionPattern(major, minor, patch, suffix) =>
        Some(StableVersion(major.toInt, minor.toInt, patch.toInt, Option(suffix)))
      case _ => None

  trait Version extends Comparable[Version]:
    final def isLessOrEqualThan(t: Version): Boolean = compareTo(t) <= 0

  case object DevVersion extends Version:
    override def toString: String = "dev"

    override def compareTo(t: Version): Int = t match
      case DevVersion => 0
      case _: StableVersion => 1

  case class StableVersion(major: Int, minor: Int, patch: Int, suffix: Option[String]) extends Version:
    override def toString: String = f"$major.$minor.$patch${suffix.getOrElse("")}"

    override def compareTo(t: Version): Int = t match
      case DevVersion => -1
      case StableVersion(major2, minor2, patch2, suffix2) =>
        val majorCmp = major.compareTo(major2)
        if majorCmp != 0 then majorCmp
        else
          val minorCmp = minor.compareTo(minor2)
          if minorCmp != 0 then minorCmp
          else
            val patchCmp = patch.compareTo(patch2)
            if patchCmp != 0 then patchCmp
            else
              suffix match
                case Some(s) =>
                  suffix2 match
                    case Some(s2) => s.compareTo(s2)
                    case None => 1
                case None =>
                  suffix2 match
                    case Some(_) => -1
                    case None => 0
