from jinja2 import Environment, FileSystemLoader
import yaml
import logging
from discord_webhook import DiscordWebhook, DiscordEmbed

from yaml.loader import SafeLoader


def get_logger(name, today, hour):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(f"log/{today}/{name}_{str(hour)}.log")
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(levelname)s - %(asctime)s - %(message)s")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def load_config(path):
    """
    Load config

    :param path: path
    :return:
    """
    file_path = path
    with open(file_path) as f:
        data = yaml.load(f, Loader=SafeLoader)
        return data


def render_sql(template_file_name, **params):
    """
    Render SQL using template file

    :param template_file_name
    :param params:
    :return:
    """
    loader = FileSystemLoader(searchpath="")
    env = Environment(loader=loader)
    template = env.get_template(template_file_name)

    return template.render(**params)


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def send_except_alert(run_env, errors, title):
    conf = load_config('config/marketing_system_config.yaml')
    webhook = DiscordWebhook(url=conf[run_env]['webhook'], username="Pipeline Ingest Alert")
    embed = DiscordEmbed(title=title, color=242424)
    embed.set_timestamp()
    embed.add_embed_field(name="Project", value="Pipeline Ingest Raw Data", inline=False)
    embed.add_embed_field(name="Errors", value=errors, inline=False)
    webhook.add_embed(embed)
    webhook.execute()


def send_logics_alert(run_env, errors, title, msg):
    conf = load_config('config/marketing_system_config.yaml')
    webhook = DiscordWebhook(url=conf[run_env]['webhook'], username="Pipeline Ingest Alert")
    embed = DiscordEmbed(title=title, color=242424)
    embed.set_timestamp()
    embed.add_embed_field(name="Project", value="Pipeline Ingest Raw Data", inline=False)
    embed.add_embed_field(name="Message", value=msg, inline=False)
    embed.add_embed_field(name="Errors", value=errors, inline=False)
    webhook.add_embed(embed)
    webhook.execute()
